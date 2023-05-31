package au.org.ala.pipelines.beam;

import static au.org.ala.pipelines.transforms.IndexFields.*;
import static au.org.ala.pipelines.transforms.IndexValues.*;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.ALL_AVRO;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.common.SolrFieldSchema;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.SolrPipelineOptions;
import au.org.ala.pipelines.transforms.IndexFields;
import au.org.ala.pipelines.transforms.IndexRecordTransform;
import au.org.ala.pipelines.transforms.IndexValues;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import avro.shaded.com.google.common.collect.ImmutableMap;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.*;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.slf4j.MDC;

/**
 * Pipeline that joins index records with:
 *
 * <ul>
 *   <li>sample data (environmental and contextual properties)
 *   <li>jackknife environmental outlier information
 *   <li>clustering (duplicate detection)
 *   <li>expert distribution outlier information and then indexes to SOLR.
 * </ul>
 *
 * <p>Jackknife, clustering and expert distribution are joined with IndexRecords in a single
 * CoGroupByKey as they are all using occurrenceIDs. These output are generated in other pipelines
 * which are run prior to running this one.
 *
 * <p>Sampling is joined to IndexRecords using a latitude_longitude string.
 */
@Slf4j
public class IndexRecordToSolrPipeline {

  private static final SampleRecord nullSampling =
      SampleRecord.newBuilder().setLatLng("NO_VALUE").build();

  public static final String EMPTY = "EMPTY";
  static final IndexRecord nullIndexRecord = IndexRecord.newBuilder().setId(EMPTY).build();
  static final JackKnifeOutlierRecord nullJkor =
      JackKnifeOutlierRecord.newBuilder().setId(EMPTY).setItems(new ArrayList<>()).build();
  static final Relationships nullClustering =
      Relationships.newBuilder().setId(EMPTY).setRelationships(new ArrayList<>()).build();
  static final DistributionOutlierRecord nullOutlier =
      DistributionOutlierRecord.newBuilder().setId(EMPTY).build();

  static final RecordAnnotations nullAnnotations =
      RecordAnnotations.newBuilder().setId(EMPTY).setAnnotations(new ArrayList<>()).build();

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    MDC.put("step", "INDEX_RECORD_TO_SOLR");

    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "solr");
    SolrPipelineOptions options =
        PipelinesOptionsFactory.create(SolrPipelineOptions.class, combinedArgs);
    MDC.put("datasetId", options.getDatasetId() != null ? options.getDatasetId() : "ALL_RECORDS");
    options.setMetaFileName(ValidationUtils.INDEXING_METRICS);
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(SolrPipelineOptions options) {

    final Map<String, SolrFieldSchema> schemaFields = getSchemaFields(options);
    final List<String> dynamicFieldPrefixes = getSchemaDynamicFieldPrefixes(options);
    final int numOfPartitions = options.getNumOfPartitions();

    Pipeline pipeline = Pipeline.create(options);

    // Load IndexRecords - keyed on UUID
    PCollection<KV<String, IndexRecord>> indexRecordsCollection =
        loadIndexRecords(options, pipeline);
    PCollection<KV<String, JackKnifeOutlierRecord>> jackKnife =
        loadJackKnifeRecords(options, pipeline);
    PCollection<KV<String, Relationships>> clusters = loadClusteringRecords(options, pipeline);
    PCollection<KV<String, DistributionOutlierRecord>> outliers =
        loadOutlierRecords(options, pipeline);
    PCollection<KV<String, RecordAnnotations>> annotations =
        loadAnnotationRecords(options, pipeline);

    // Co group IndexRecords with coordinates with Sample data
    final TupleTag<IndexRecord> indexRecordTag = new TupleTag<>();
    final TupleTag<JackKnifeOutlierRecord> jackknifeTag = new TupleTag<>();
    final TupleTag<Relationships> clusterTag = new TupleTag<>();
    final TupleTag<DistributionOutlierRecord> outliersTag = new TupleTag<>();
    final TupleTag<RecordAnnotations> annotationsTag = new TupleTag<>();

    // Join collections by LatLng string
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(indexRecordTag, indexRecordsCollection)
            .and(jackknifeTag, jackKnife)
            .and(clusterTag, clusters)
            .and(outliersTag, outliers)
            .and(annotationsTag, annotations)
            .apply(CoGroupByKey.create());

    // run the CoGroupByKey
    indexRecordsCollection =
        results.apply(
            ParDo.of(
                joinProcessing(
                    indexRecordTag, jackknifeTag, clusterTag, outliersTag, annotationsTag)));

    PCollection<IndexRecord> readyToIndex = null;

    if (options.getIncludeSampling()) {

      log.info("Adding sampling to the index");

      // Load Samples - keyed on LatLng
      PCollection<KV<String, SampleRecord>> sampleRecords =
          loadSampleRecords(options, pipeline, numOfPartitions);

      // Convert to KV <LatLng, IndexRecord>
      PCollection<KV<String, IndexRecord>> indexRecordsKeyedLatng =
          indexRecordsCollection.apply(
              MapElements.via(
                  new SimpleFunction<KV<String, IndexRecord>, KV<String, IndexRecord>>() {
                    @Override
                    public KV<String, IndexRecord> apply(KV<String, IndexRecord> input) {
                      // add hash
                      Random ran = new Random();
                      // values 0 to numOfPartitions
                      int x = ran.nextInt(numOfPartitions - 1);
                      String latLng =
                          Strings.isEmpty(input.getValue().getLatLng())
                              ? input
                                  .getValue()
                                  .getId() // just need a unique ID so there is no join
                              : x + "-" + input.getValue().getLatLng();
                      return KV.of(latLng, input.getValue());
                    }
                  }));

      // add sampling to the records with coordinates
      readyToIndex = joinSampleRecord(indexRecordsKeyedLatng, sampleRecords);
      SolrIO.ConnectionConfiguration conn =
          SolrIO.ConnectionConfiguration.create(options.getZkHost());

      writeToSolr(options, readyToIndex, conn, schemaFields, dynamicFieldPrefixes);

    } else {
      readyToIndex = indexRecordsCollection.apply(Values.create());
      log.info("Adding step 4: Create SOLR connection");
      SolrIO.ConnectionConfiguration conn =
          SolrIO.ConnectionConfiguration.create(options.getZkHost());
      writeToSolr(options, readyToIndex, conn, schemaFields, dynamicFieldPrefixes);
    }

    log.info("Starting pipeline");
    pipeline.run(options).waitUntilFinish();

    log.info("Solr indexing pipeline complete");
  }

  @NotNull
  private static Map<String, SolrFieldSchema> getSchemaFields(SolrPipelineOptions options) {
    try (CloudSolrClient client =
        new CloudSolrClient.Builder(ImmutableList.of(options.getZkHost()), Optional.empty())
            .build()) {
      SchemaRequest.Fields fields = new SchemaRequest.Fields();
      SchemaResponse.FieldsResponse response = fields.process(client, options.getSolrCollection());
      Map<String, SolrFieldSchema> schema = new HashMap<String, SolrFieldSchema>();
      for (Map<String, Object> field : response.getFields()) {
        schema.put(
            (String) field.get("name"),
            new SolrFieldSchema(
                (String) field.get("type"), (boolean) field.getOrDefault("multiValued", false)));
      }
      return schema;
    } catch (Exception e) {
      throw new PipelinesException("Unable to retrieve schema fields: " + e.getMessage());
    }
  }

  @NotNull
  private static List<String> getSchemaDynamicFieldPrefixes(SolrPipelineOptions options) {
    try (CloudSolrClient client =
        new CloudSolrClient.Builder(ImmutableList.of(options.getZkHost()), Optional.empty())
            .build()) {
      SchemaRequest.DynamicFields fields = new SchemaRequest.DynamicFields();
      SchemaResponse.DynamicFieldsResponse response =
          fields.process(client, options.getSolrCollection());
      return response.getDynamicFields().stream()
          .map(f -> f.get("name").toString().replace("*", ""))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new PipelinesException("Unable to retrieve schema fields: " + e.getMessage());
    }
  }

  private static PCollection<IndexRecord> joinSampleRecord(
      PCollection<KV<String, IndexRecord>> indexRecords,
      PCollection<KV<String, SampleRecord>> sampleRecords) {

    // Co group IndexRecords with coordinates with Sample data
    final TupleTag<IndexRecord> indexRecordTag = new TupleTag<>();
    final TupleTag<SampleRecord> samplingTag = new TupleTag<>();

    // Join collections by LatLng string
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(indexRecordTag, indexRecords)
            .and(samplingTag, sampleRecords)
            .apply(CoGroupByKey.create());

    // Create collection which contains samples keyed with indexRecord.id
    return results.apply(ParDo.of(joinSampling(indexRecordTag, samplingTag)));
  }

  private static void writeToSolr(
      SolrPipelineOptions options,
      PCollection<IndexRecord> indexRecords,
      SolrIO.ConnectionConfiguration conn,
      final Map<String, SolrFieldSchema> schemaFields,
      final List<String> dynamicFieldPrefixes) {

    if (options.getOutputAvroToFilePath() == null) {

      indexRecords
          .apply(
              "IndexRecord to SOLR Document",
              ParDo.of(
                  new DoFn<IndexRecord, SolrInputDocument>() {
                    @DoFn.ProcessElement
                    public void processElement(
                        @DoFn.Element IndexRecord indexRecord,
                        OutputReceiver<SolrInputDocument> out) {
                      SolrInputDocument solrInputDocument =
                          IndexRecordTransform.convertIndexRecordToSolrDoc(
                              indexRecord, schemaFields, dynamicFieldPrefixes);
                      out.output(solrInputDocument);
                    }
                  }))
          .apply(
              SolrIO.write()
                  .to(options.getSolrCollection())
                  .withConnectionConfiguration(conn)
                  .withMaxBatchSize(options.getSolrBatchSize())
                  .withRetryConfiguration(
                      SolrIO.RetryConfiguration.create(
                          options.getSolrRetryMaxAttempts(),
                          Duration.standardMinutes(options.getSolrRetryDurationInMins()))));
    } else {
      indexRecords.apply(AvroIO.write(IndexRecord.class).to(options.getOutputAvroToFilePath()));
    }
  }

  private static void addOutlierInfo(
      IndexRecord indexRecord, DistributionOutlierRecord outlierRecord) {
    indexRecord
        .getDoubles()
        .put(DISTANCE_FROM_EXPERT_DISTRIBUTION, outlierRecord.getDistanceOutOfEDL());
  }

  private static void addRecordAnnotationInfo(
      IndexRecord indexRecord, RecordAnnotations annotationsRecord) {
    indexRecord.setAnnotations(annotationsRecord.getAnnotations());
    indexRecord
        .getMultiValues()
        .put(
            ANNOTATIONS_DOI,
            annotationsRecord.getAnnotations().stream()
                .map(a -> a.getDoi())
                .collect(Collectors.toList()));
    indexRecord
        .getMultiValues()
        .put(
            ANNOTATIONS_UID,
            annotationsRecord.getAnnotations().stream()
                .map(a -> a.getDatasetKey())
                .collect(Collectors.toList()));
  }

  private static void addJackKnifeInfo(IndexRecord indexRecord, JackKnifeOutlierRecord jkor) {

    Map<String, Integer> ints = indexRecord.getInts();

    if (ints == null) {
      ints = new HashMap<>();
      indexRecord.setInts(ints);
    }

    if (jkor != nullJkor && !EMPTY.equals(jkor.getId())) {

      ints.put(OUTLIER_LAYER_COUNT, jkor.getItems().size());

      Map<String, List<String>> multiValues = indexRecord.getMultiValues();
      if (multiValues == null) {
        multiValues = new HashMap<>();
        indexRecord.setMultiValues(multiValues);
      }

      multiValues.put(OUTLIER_LAYER, jkor.getItems());
    } else {
      ints.put(OUTLIER_LAYER_COUNT, 0);
    }
  }

  private static void addClusteringInfo(IndexRecord indexRecord, Relationships jkor) {

    String id = indexRecord.getId();

    Map<String, List<String>> multiValues = indexRecord.getMultiValues();
    Map<String, Boolean> booleans = indexRecord.getBooleans();
    Map<String, String> strings = indexRecord.getStrings();
    Map<String, Integer> ints = indexRecord.getInts();

    if (multiValues == null) {
      multiValues = new HashMap<>();
      indexRecord.setMultiValues(multiValues);
    }

    if (booleans == null) {
      booleans = new HashMap<>();
      indexRecord.setBooleans(booleans);
    }

    if (strings == null) {
      strings = new HashMap<>();
      indexRecord.setStrings(strings);
    }

    if (ints == null) {
      ints = new HashMap<>();
      indexRecord.setInts(ints);
    }

    if (jkor != nullClustering
        && !EMPTY.equals(jkor.getId())
        && !jkor.getRelationships().isEmpty()) {

      booleans.put(IS_IN_CLUSTER, true);

      boolean isRepresentative = false;

      Set<String> duplicateType = new HashSet<>();
      for (Relationship relationship : jkor.getRelationships()) {

        // A record may end up being marked as both associative
        // and representative in two or more separate clusters.
        // The important thing here is that if it is marked
        // as representative in any relationship we mark is as such
        // as the sole purpose of representative/associative markers
        // is to allow users to filter out duplicates.
        boolean linkingError = false;
        if (relationship.getRepId().equals(id)) {
          isRepresentative = true;
        }

        if (relationship.getDupDataset().equals(relationship.getRepDataset())) {
          duplicateType.add(SAME_DATASET);
        } else if (!linkingError) {
          duplicateType.add(DIFFERENT_DATASET);
        } else {
          duplicateType.add(LINKING_ERROR);
        }
      }

      String duplicateStatus = IndexValues.ASSOCIATED;
      if (isRepresentative) {
        duplicateStatus = IndexValues.REPRESENTATIVE;
      }

      // a record may be representative of several records
      List<String> isRepresentativeOf =
          jkor.getRelationships().stream()
              .map(Relationship::getDupId)
              .distinct()
              .filter(recordId -> !recordId.equals(id))
              .collect(Collectors.toList());

      // a record is a duplicate of a single representative record
      List<Relationship> isDuplicateOf =
          jkor.getRelationships().stream()
              .distinct()
              .filter(relationship -> relationship.getDupId().equals(id))
              .collect(Collectors.toList());

      if (!isRepresentativeOf.isEmpty()) {
        multiValues.put(IndexFields.IS_REPRESENTATIVE_OF, isRepresentativeOf);
        strings.put(
            DwcTerm.associatedOccurrences.simpleName(), String.join("|", isRepresentativeOf));
      }

      if (!isDuplicateOf.isEmpty()) {
        strings.put(IS_DUPLICATE_OF, isDuplicateOf.get(0).getRepId());
        String[] justification = isDuplicateOf.get(0).getJustification().split(",");
        multiValues.put(DUPLICATE_JUSTIFICATION, Arrays.asList(justification));
        strings.put(DwcTerm.associatedOccurrences.simpleName(), isDuplicateOf.get(0).getRepId());
      }

      // set the status
      strings.put(DUPLICATE_STATUS, duplicateStatus);

      // add duplicate types
      List<String> duplicateTypeList = new ArrayList<>(duplicateType);
      multiValues.put(DUPLICATE_TYPE, duplicateTypeList);

    } else {
      booleans.put(IS_IN_CLUSTER, false);
    }
  }

  private static DoFn<KV<String, CoGbkResult>, IndexRecord> joinSampling(
      TupleTag<IndexRecord> indexRecordTag, TupleTag<SampleRecord> samplingTag) {

    return new DoFn<KV<String, CoGbkResult>, IndexRecord>() {

      @ProcessElement
      public void processElement(ProcessContext c) {

        KV<String, CoGbkResult> e = c.element();

        Iterable<IndexRecord> indexRecordIterable = e.getValue().getAll(indexRecordTag);
        SampleRecord sampleRecord = e.getValue().getOnly(samplingTag, nullSampling);

        indexRecordIterable.forEach(
            indexRecord -> {
              if (sampleRecord != null && !sampleRecord.equals(nullSampling)) {

                Map<String, String> strings =
                    indexRecord.getStrings() != null ? indexRecord.getStrings() : new HashMap<>();
                Map<String, Double> doubles =
                    indexRecord.getDoubles() != null ? indexRecord.getDoubles() : new HashMap<>();

                Map<String, String> stringsToPersist =
                    ImmutableMap.<String, String>builder()
                        .putAll(strings)
                        .putAll(sampleRecord.getStrings())
                        .build();

                Map<String, Double> doublesToPersist =
                    ImmutableMap.<String, Double>builder()
                        .putAll(doubles)
                        .putAll(sampleRecord.getDoubles())
                        .build();

                IndexRecord ir =
                    IndexRecord.newBuilder()
                        .setId(indexRecord.getId())
                        .setTaxonID(indexRecord.getTaxonID())
                        .setLatLng(indexRecord.getLatLng())
                        .setMultiValues(indexRecord.getMultiValues())
                        .setDates(indexRecord.getDates())
                        .setLongs(indexRecord.getLongs())
                        .setBooleans(indexRecord.getBooleans())
                        .setInts(indexRecord.getInts())
                        .setStrings(stringsToPersist)
                        .setDoubles(doublesToPersist)
                        .setDynamicProperties(indexRecord.getDynamicProperties())
                        .build();
                c.output(ir);
              } else {
                c.output(indexRecord);
              }
            });
      }
    };
  }

  /**
   * Join processing outputs which are all key-ed on OccurrenceID.
   *
   * @param indexRecordTag
   * @param jackKnifeTag
   * @param clusteringTag
   * @param outlierTag
   * @return
   */
  private static DoFn<KV<String, CoGbkResult>, KV<String, IndexRecord>> joinProcessing(
      TupleTag<IndexRecord> indexRecordTag,
      TupleTag<JackKnifeOutlierRecord> jackKnifeTag,
      TupleTag<Relationships> clusteringTag,
      TupleTag<DistributionOutlierRecord> outlierTag,
      TupleTag<RecordAnnotations> annotationsTag) {

    return new DoFn<KV<String, CoGbkResult>, KV<String, IndexRecord>>() {

      @ProcessElement
      public void processElement(ProcessContext c) {

        KV<String, CoGbkResult> e = c.element();
        IndexRecord indexRecord = e.getValue().getOnly(indexRecordTag, nullIndexRecord);

        if (indexRecord != null && !indexRecord.equals(nullIndexRecord)) {

          JackKnifeOutlierRecord jackKnife = e.getValue().getOnly(jackKnifeTag, nullJkor);
          Relationships clustering = e.getValue().getOnly(clusteringTag, nullClustering);
          DistributionOutlierRecord outlierRecord = e.getValue().getOnly(outlierTag, nullOutlier);
          RecordAnnotations annotationsRecord =
              e.getValue().getOnly(annotationsTag, nullAnnotations);

          if (jackKnife != null) {
            addJackKnifeInfo(indexRecord, jackKnife);
          }

          if (clustering != null) {
            addClusteringInfo(indexRecord, clustering);
          }

          if (outlierRecord != null) {
            addOutlierInfo(indexRecord, outlierRecord);
          }

          if (annotationsRecord != null) {
            addRecordAnnotationInfo(indexRecord, annotationsRecord);
          }

          c.output(KV.of(indexRecord.getId(), indexRecord));
        } else {
          log.error("Join null for key " + e.getKey());
        }
      }
    };
  }

  /** Load index records from AVRO. */
  private static PCollection<KV<String, IndexRecord>> loadIndexRecords(
      SolrPipelineOptions options, Pipeline p) {
    return ALAFsUtils.loadIndexRecords(options, p)
        .apply(
            MapElements.via(
                new SimpleFunction<IndexRecord, KV<String, IndexRecord>>() {
                  @Override
                  public KV<String, IndexRecord> apply(IndexRecord input) {
                    return KV.of(input.getId(), input);
                  }
                }));
  }

  private static PCollection<KV<String, SampleRecord>> loadSampleRecords(
      AllDatasetsPipelinesOptions options, Pipeline p, final int partitions) {
    String samplingPath =
        String.join("/", ALAFsUtils.buildPathSamplingUsingTargetPath(options), "*.avro");
    log.info("Loading sampling from {}", samplingPath);
    return p.apply(AvroIO.read(SampleRecord.class).from(samplingPath))
        .apply(
            ParDo.of(
                new DoFn<SampleRecord, KV<String, SampleRecord>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    SampleRecord s = c.element();
                    for (int i = 0; i < partitions; i++) {
                      SampleRecord s1 =
                          SampleRecord.newBuilder()
                              .setLatLng(s.getLatLng())
                              .setDoubles(s.getDoubles())
                              .setStrings(s.getStrings())
                              .build();
                      c.output(KV.of(i + "-" + s.getLatLng(), s1));
                    }
                  }
                }));
  }

  private static PCollection<KV<String, JackKnifeOutlierRecord>> loadJackKnifeRecords(
      SolrPipelineOptions options, Pipeline p) {

    if (!options.getIncludeJackKnife()) {
      return p.apply(Create.empty(new TypeDescriptor<KV<String, JackKnifeOutlierRecord>>() {}));
    }

    String jackknifePath =
        PathBuilder.buildPath(options.getJackKnifePath(), "outliers", ALL_AVRO).toString();
    log.info("Loading jackknife from {}", jackknifePath);
    return p.apply(
            AvroIO.read(JackKnifeOutlierRecord.class)
                .from(jackknifePath)
                .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
        .apply(
            MapElements.via(
                new SimpleFunction<JackKnifeOutlierRecord, KV<String, JackKnifeOutlierRecord>>() {
                  @Override
                  public KV<String, JackKnifeOutlierRecord> apply(JackKnifeOutlierRecord input) {
                    return KV.of(input.getId(), input);
                  }
                }));
  }

  private static PCollection<KV<String, Relationships>> loadClusteringRecords(
      SolrPipelineOptions options, Pipeline p) {

    if (!options.getIncludeClustering()) {
      return p.apply(Create.empty(new TypeDescriptor<KV<String, Relationships>>() {}));
    }

    String path =
        PathBuilder.buildPath(
                options.getClusteringPath() + "/relationships/", "relationships-*" + AVRO_EXTENSION)
            .toString();
    log.info("Loading clustering from {}", path);

    return p.apply(
            AvroIO.read(Relationships.class)
                .from(path)
                .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
        .apply(
            MapElements.via(
                new SimpleFunction<Relationships, KV<String, Relationships>>() {
                  @Override
                  public KV<String, Relationships> apply(Relationships input) {
                    return KV.of(input.getId(), input);
                  }
                }));
  }

  private static PCollection<KV<String, DistributionOutlierRecord>> loadOutlierRecords(
      SolrPipelineOptions options, Pipeline p) {

    if (!options.getIncludeOutlier()) {
      return p.apply(Create.empty(new TypeDescriptor<KV<String, DistributionOutlierRecord>>() {}));
    }

    String dataResourceFolder = options.getDatasetId();
    if (dataResourceFolder == null || "all".equalsIgnoreCase(dataResourceFolder)) {
      dataResourceFolder = "all";
    }

    String path =
        PathBuilder.buildPath(options.getOutlierPath(), dataResourceFolder, ALL_AVRO).toString();
    log.info("Loading outlier from {}", path);

    return p.apply(
            AvroIO.read(DistributionOutlierRecord.class)
                .from(path)
                .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
        .apply(
            MapElements.via(
                new SimpleFunction<
                    DistributionOutlierRecord, KV<String, DistributionOutlierRecord>>() {
                  @Override
                  public KV<String, DistributionOutlierRecord> apply(
                      DistributionOutlierRecord input) {
                    return KV.of(input.getId(), input);
                  }
                }));
  }

  private static PCollection<KV<String, RecordAnnotations>> loadAnnotationRecords(
      SolrPipelineOptions options, Pipeline p) {

    String path =
        String.join(
            Path.SEPARATOR,
            options.getAnnotationsPath(),
            "*",
            options.getAttempt().toString(),
            ALL_AVRO);

    log.info("Loading annotations from {}", path);

    PCollection<KV<String, RecordAnnotation>> annotationsNonGrouped =
        p.apply(
                AvroIO.read(RecordAnnotation.class)
                    .from(path)
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
            .apply(
                MapElements.via(
                    new SimpleFunction<RecordAnnotation, KV<String, RecordAnnotation>>() {
                      @Override
                      public KV<String, RecordAnnotation> apply(RecordAnnotation input) {
                        return KV.of(input.getId(), input);
                      }
                    }));

    // group by ID
    PCollection<KV<String, Iterable<RecordAnnotation>>> grouped =
        annotationsNonGrouped.apply(GroupByKey.create());

    // convert to RecordAnnotation
    return grouped.apply(
        MapElements.via(
            new SimpleFunction<
                KV<String, Iterable<RecordAnnotation>>, KV<String, RecordAnnotations>>() {
              @Override
              public KV<String, RecordAnnotations> apply(
                  KV<String, Iterable<RecordAnnotation>> input) {
                List<RecordAnnotation> annotationList = new ArrayList<>();
                input.getValue().forEach(annotationList::add);
                RecordAnnotations annotations =
                    RecordAnnotations.newBuilder()
                        .setAnnotations(annotationList)
                        .setId(input.getKey())
                        .build();
                return KV.of(annotations.getId(), annotations);
              }
            }));
  }
}
