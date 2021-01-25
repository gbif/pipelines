package au.org.ala.pipelines.beam;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import au.org.ala.pipelines.options.SolrPipelineOptions;
import au.org.ala.pipelines.transforms.IndexRecordTransform;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import avro.shaded.com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.*;
import org.joda.time.Duration;
import org.slf4j.MDC;

/** Pipeline that joins sample data and index records and indexes to SOLR. */
@Slf4j
public class IndexRecordToSolrPipeline {

  static final SampleRecord nullSampling = SampleRecord.newBuilder().setLatLng("NO_VALUE").build();
  static final JackKnifeOutlierRecord nullJkor =
      JackKnifeOutlierRecord.newBuilder().setId("EMPTY").setItems(new ArrayList<>()).build();
  static final Relationships nullClustering =
      Relationships.newBuilder().setId("EMPTY").setRelationships(new ArrayList<>()).build();

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
  }

  public static boolean hasCoordinates(IndexRecord indexRecord) {
    return indexRecord.getLatLng() != null;
  }

  public static void run(SolrPipelineOptions options) {

    Pipeline pipeline = Pipeline.create(options);

    // Load IndexRecords - keyed on UUID
    PCollection<KV<String, IndexRecord>> indexRecordsCollection =
        loadIndexRecords(options, pipeline);

    // If configured to do so, include jack knife information
    if (options.getIncludeJackKnife()) {
      log.info("Adding jack knife to the index");
      indexRecordsCollection = addJacknifeInfo(options, pipeline, indexRecordsCollection);
    } else {
      log.info("Skipping adding jack knife to the index");
    }

    // If configured to do so, include jack knife information
    if (options.getIncludeClustering()) {
      log.info("Adding clustering to the index");
      indexRecordsCollection = addClusteringInfo(options, pipeline, indexRecordsCollection);
    } else {
      log.info("Skipping adding clustering to the index");
    }

    PCollection<KV<String, IndexRecord>> recordsWithoutCoordinates = null;

    if (options.getIncludeSampling()) {

      log.info("Adding sampling to the index");

      // Load Samples - keyed on LatLng
      PCollection<KV<String, SampleRecord>> sampleRecords = loadSampleRecords(options, pipeline);

      // split into records with coordinates and records without
      // Filter records with coordinates - we will join these to samples
      PCollection<KV<String, IndexRecord>> recordsWithCoordinates =
          indexRecordsCollection.apply(
              Filter.by(indexRecord -> hasCoordinates(indexRecord.getValue())));

      // Filter records without coordinates - we will index, but not sample these
      recordsWithoutCoordinates =
          indexRecordsCollection.apply(
              Filter.by(indexRecord -> !hasCoordinates(indexRecord.getValue())));

      // Convert to KV <LatLng, IndexRecord>
      PCollection<KV<String, IndexRecord>> recordsWithCoordinatesKeyedLatng =
          recordsWithCoordinates.apply(
              MapElements.via(
                  new SimpleFunction<KV<String, IndexRecord>, KV<String, IndexRecord>>() {
                    @Override
                    public KV<String, IndexRecord> apply(KV<String, IndexRecord> input) {
                      String latLng =
                          input.getValue().getLatLng() == null
                              ? "NO_LAT_LNG"
                              : input.getValue().getLatLng();
                      return KV.of(latLng, input.getValue());
                    }
                  }));

      // Co group IndexRecords with coordinates with Sample data
      final TupleTag<IndexRecord> indexRecordTag = new TupleTag<>();
      final TupleTag<SampleRecord> samplingTag = new TupleTag<>();

      // Join collections by LatLng string
      PCollection<KV<String, CoGbkResult>> results =
          KeyedPCollectionTuple.of(samplingTag, sampleRecords)
              .and(indexRecordTag, recordsWithCoordinatesKeyedLatng)
              .apply(CoGroupByKey.create());

      // Create collection which contains samples keyed with indexRecord.id
      indexRecordsCollection = results.apply(ParDo.of(joinSampling(indexRecordTag, samplingTag)));
    } else {
      log.info("Skipping adding sampling");
    }

    log.info("Adding step 4: Create SOLR connection");
    SolrIO.ConnectionConfiguration conn =
        SolrIO.ConnectionConfiguration.create(options.getZkHost());

    log.info("Adding step 5: Write records to SOLR");
    writeToSolr(options, indexRecordsCollection, conn);

    if (recordsWithoutCoordinates != null) {
      log.info("Adding step 5: Write records (without coordinates) to SOLR");
      writeToSolr(options, recordsWithoutCoordinates, conn);
    }

    log.info("Starting pipeline");
    pipeline.run(options).waitUntilFinish();

    log.info("Solr indexing pipeline complete");
  }

  private static PCollection<KV<String, IndexRecord>> addJacknifeInfo(
      SolrPipelineOptions options,
      Pipeline pipeline,
      PCollection<KV<String, IndexRecord>> indexRecords) {

    // Load Jackknife, keyed on ID
    PCollection<KV<String, JackKnifeOutlierRecord>> jackKnifeRecordsKeyedRecordID =
        loadJackKnifeRecords(options, pipeline);

    // Join indexRecordsSampling and jackKnife
    PCollection<KV<String, KV<IndexRecord, JackKnifeOutlierRecord>>>
        indexRecordSamplingJoinJackKnife =
            Join.leftOuterJoin(indexRecords, jackKnifeRecordsKeyedRecordID, nullJkor);

    // Add Jackknife information
    return indexRecordSamplingJoinJackKnife.apply(ParDo.of(addJackknifeInfo()));
  }

  private static PCollection<KV<String, IndexRecord>> addClusteringInfo(
      SolrPipelineOptions options,
      Pipeline pipeline,
      PCollection<KV<String, IndexRecord>> indexRecords) {

    // Load clustering, keyed on ID
    PCollection<KV<String, Relationships>> clusteringRecordsKeyedRecordID =
        loadClusteringRecords(options, pipeline);

    // Join indexRecordsSampling and jackKnife
    PCollection<KV<String, KV<IndexRecord, Relationships>>> indexRecordJoinClustering =
        Join.leftOuterJoin(indexRecords, clusteringRecordsKeyedRecordID, nullClustering);

    // Add Jackknife information
    return indexRecordJoinClustering.apply(ParDo.of(addClusteringInfo()));
  }

  private static void writeToSolr(
      SolrPipelineOptions options,
      PCollection<KV<String, IndexRecord>> kvIndexRecords,
      SolrIO.ConnectionConfiguration conn) {
    kvIndexRecords
        .apply("SOLR_doc", ParDo.of(new IndexRecordTransform.KVIndexRecordToSolrInputDocumentFcn()))
        .apply(
            SolrIO.write()
                .to(options.getSolrCollection())
                .withConnectionConfiguration(conn)
                .withMaxBatchSize(options.getSolrBatchSize())
                .withRetryConfiguration(
                    SolrIO.RetryConfiguration.create(
                        options.getSolrRetryMaxAttempts(),
                        Duration.standardMinutes(options.getSolrRetryDurationInMins()))));
  }

  private static DoFn<KV<String, KV<IndexRecord, JackKnifeOutlierRecord>>, KV<String, IndexRecord>>
      addJackknifeInfo() {

    return new DoFn<
        KV<String, KV<IndexRecord, JackKnifeOutlierRecord>>, KV<String, IndexRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {

        KV<String, KV<IndexRecord, JackKnifeOutlierRecord>> e = c.element();
        IndexRecord indexRecord = e.getValue().getKey();
        JackKnifeOutlierRecord jkor = e.getValue().getValue();
        Map<String, Integer> ints = indexRecord.getInts();
        if (ints == null) {
          ints = new HashMap<>();
          indexRecord.setInts(ints);
        }
        if (jkor != nullJkor) {

          ints.put("outlierLayerCount", jkor.getItems().size());

          Map<String, List<String>> multiValues = indexRecord.getMultiValues();
          if (multiValues == null) {
            multiValues = new HashMap();
            indexRecord.setMultiValues(multiValues);
          }

          multiValues.put("outlierLayer", jkor.getItems());
        } else {
          ints.put("outlierLayerCount", 0);
        }

        c.output(KV.of(indexRecord.getId(), indexRecord));
      }
    };
  }

  private static DoFn<KV<String, KV<IndexRecord, Relationships>>, KV<String, IndexRecord>>
      addClusteringInfo() {

    return new DoFn<KV<String, KV<IndexRecord, Relationships>>, KV<String, IndexRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {

        KV<String, KV<IndexRecord, Relationships>> e = c.element();

        IndexRecord indexRecord = e.getValue().getKey();
        Relationships jkor = e.getValue().getValue();

        Map<String, List<String>> multiValues = indexRecord.getMultiValues();
        Map<String, Boolean> booleans = indexRecord.getBooleans();

        if (multiValues == null) {
          multiValues = new HashMap<>();
          indexRecord.setMultiValues(multiValues);
        }

        if (booleans == null) {
          booleans = new HashMap<>();
          indexRecord.setBooleans(booleans);
        }

        if (jkor != nullClustering) {

          booleans.put("isClustered", true);

          List<String> id1s =
              jkor.getRelationships().stream()
                  .map(Relationship::getId1)
                  .collect(Collectors.toList());
          List<String> id2s =
              jkor.getRelationships().stream()
                  .map(Relationship::getId2)
                  .collect(Collectors.toList());

          id1s.addAll(id2s);
          id1s.remove(indexRecord.getId());

          multiValues.put("clusteredWith", id1s);

        } else {
          booleans.put("isClustered", false);
        }

        c.output(KV.of(indexRecord.getId(), indexRecord));
      }
    };
  }

  private static DoFn<KV<String, CoGbkResult>, KV<String, IndexRecord>> joinSampling(
      TupleTag<IndexRecord> indexRecordTag, TupleTag<SampleRecord> samplingTag) {

    return new DoFn<KV<String, CoGbkResult>, KV<String, IndexRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext c) {

        KV<String, CoGbkResult> e = c.element();

        SampleRecord sampleRecord = e.getValue().getOnly(samplingTag, nullSampling);
        Iterable<IndexRecord> indexRecordIterable = e.getValue().getAll(indexRecordTag);

        if (sampleRecord.getStrings() == null && sampleRecord.getDoubles() == null) {
          log.error("Sampling was empty for point: {}", e.getKey());
        }

        indexRecordIterable.forEach(
            indexRecord -> {
              Map<String, String> strings =
                  indexRecord.getStrings() != null
                      ? indexRecord.getStrings()
                      : new HashMap<String, String>();
              Map<String, Double> doubles =
                  indexRecord.getDoubles() != null
                      ? indexRecord.getDoubles()
                      : new HashMap<String, Double>();

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
                      .build();

              c.output(KV.of(indexRecord.getId(), ir));
            });
      }
    };
  }

  /**
   * Load index records from AVRO.
   *
   * @param options
   * @param p
   * @return
   */
  private static PCollection<KV<String, IndexRecord>> loadIndexRecords(
      SolrPipelineOptions options, Pipeline p) {
    if (options.getDatasetId() != null && !"all".equalsIgnoreCase(options.getDatasetId())) {
      return p.apply(
              AvroIO.read(IndexRecord.class)
                  .from(
                      String.join(
                          "/",
                          options.getAllDatasetsInputPath(),
                          "index-record",
                          options.getDatasetId() + "/*.avro")))
          .apply(
              MapElements.via(
                  new SimpleFunction<IndexRecord, KV<String, IndexRecord>>() {
                    @Override
                    public KV<String, IndexRecord> apply(IndexRecord input) {
                      return KV.of(input.getId(), input);
                    }
                  }));
    }

    return p.apply(
            AvroIO.read(IndexRecord.class)
                .from(
                    String.join(
                        "/", options.getAllDatasetsInputPath(), "index-record", "*/*.avro")))
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
      SolrPipelineOptions options, Pipeline p) {
    String samplingPath =
        String.join("/", ALAFsUtils.buildPathSamplingUsingTargetPath(options), "*.avro");
    log.info("Loading sampling from {}", samplingPath);
    return p.apply(AvroIO.read(SampleRecord.class).from(samplingPath))
        .apply(
            MapElements.via(
                new SimpleFunction<SampleRecord, KV<String, SampleRecord>>() {
                  @Override
                  public KV<String, SampleRecord> apply(SampleRecord input) {
                    return KV.of(input.getLatLng(), input);
                  }
                }));
  }

  private static PCollection<KV<String, JackKnifeOutlierRecord>> loadJackKnifeRecords(
      SolrPipelineOptions options, Pipeline p) {
    String jackknifePath =
        PathBuilder.buildPath(options.getJackKnifePath(), "outliers", "*" + AVRO_EXTENSION)
            .toString();
    log.info("Loading jackknife from {}", jackknifePath);

    return p.apply(AvroIO.read(JackKnifeOutlierRecord.class).from(jackknifePath))
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
    String path =
        PathBuilder.buildPath(options.getClusteringPath(), "*" + AVRO_EXTENSION).toString();
    log.info("Loading clustering from {}", path);

    return p.apply(AvroIO.read(Relationships.class).from(path))
        .apply(
            MapElements.via(
                new SimpleFunction<Relationships, KV<String, Relationships>>() {
                  @Override
                  public KV<String, Relationships> apply(Relationships input) {
                    return KV.of(input.getId(), input);
                  }
                }));
  }
}
