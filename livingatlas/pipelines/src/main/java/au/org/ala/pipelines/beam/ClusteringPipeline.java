package au.org.ala.pipelines.beam;

import au.org.ala.clustering.OccurrenceRelationships;
import au.org.ala.clustering.RelationshipAssertion;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.ClusteringPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.slf4j.MDC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ClusteringPipeline {

  // IDs to skip
  static final List<String> omitIds =
      Arrays.asList(
          "NO APLICA", "NA", "[]", "NO DISPONIBLE", "NO DISPONIBL", "NO NUMBER", "--", "UNKNOWN");

  // SPECIMENS
  static final List<String> specimenBORs =
      Arrays.asList("PRESERVED_SPECIMEN", "MATERIAL_SAMPLE", "LIVING_SPECIMEN", "FOSSIL_SPECIMEN");

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) throws FileNotFoundException {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "clustering");

    ClusteringPipelineOptions options =
        PipelinesOptionsFactory.create(ClusteringPipelineOptions.class, combinedArgs);

    options.setMetaFileName(ValidationUtils.CLUSTERING_METRICS);

    // JackKnife is run across all datasets.
    options.setDatasetId("*");

    MDC.put("datasetId", "*");
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "CLUSTERING");

    PipelinesOptionsFactory.registerHdfs(options);

    run(options);
  }

  public static void run(ClusteringPipelineOptions options) {

    log.info("Creating a pipeline from options");
    Pipeline pipeline = Pipeline.create(options);

    // clear previous runs
    clearPreviousClustering(options);

    // read index records
    PCollection<IndexRecord> indexRecords = loadIndexRecords(options, pipeline);

    // create hashes for everything
    PCollection<OccurrenceFeatures> hashAll =
        indexRecords
            .apply(
                ParDo.of(
                    new DoFn<IndexRecord, OccurrenceFeatures>() {
                      @ProcessElement
                      public void processElement(
                          @Element IndexRecord source, OutputReceiver<OccurrenceFeatures> out) {

                        String datasetKey = source.getStrings().get("dataResourceUid");
                        if (datasetKey == null) {
                          log.error("datasetKey null for record " + source.getId());
                          return;
                        }

                        OccurrenceFeatures.Builder builder =
                            OccurrenceFeatures.newBuilder()
                                .setId(source.getId())
                                .setDatasetKey(datasetKey)
                                .setSpeciesKey(source.getStrings().get("speciesID"))
                                .setTaxonKey(source.getStrings().get("taxonConceptID"))
                                .setBasisOfRecord(source.getStrings().get("basisOfRecord"))
                                .setDecimalLatitude(source.getDoubles().get("decimalLatitude"))
                                .setDecimalLongitude(source.getDoubles().get("decimalLongitude"))
                                .setYear(source.getInts().get("year"))
                                .setMonth(source.getInts().get("month"))
                                .setDay(source.getInts().get("day"))
                                .setEventDate(source.getLongs().get("eventDate"))
                                .setTypeStatus(source.getStrings().get("typeStatus"))
                                .setRecordedBy(source.getStrings().get("recordedBy"))
                                .setRecordedBy(source.getStrings().get("fieldNumber"))
                                .setRecordNumber(source.getStrings().get("recordNumber"))
                                .setCatalogNumber(source.getStrings().get("catalogNumber"))
                                .setOccurrenceID(source.getStrings().get("occurrenceID"))
                                .setOtherCatalogNumbers(
                                    source.getStrings().get("otherCatalogNumbers"))
                                .setCollectionCode(source.getStrings().get("collectionCode"))
                                .setInstitutionCode(source.getStrings().get("institutionCode"));

                        // specimen only hashes
                        if (Strings.isNotEmpty(builder.getSpeciesKey())
                            && Strings.isNotEmpty(builder.getBasisOfRecord())
                            && specimenBORs.contains(builder.getBasisOfRecord())) {

                          // output hashes for each combination
                          Arrays.asList(
                                  builder.getOccurrenceID(),
                                  builder.getFieldNumber(),
                                  builder.getRecordNumber(),
                                  builder.getCatalogNumber(),
                                  builder.getOtherCatalogNumbers())
                              .stream()
                              .filter(
                                  value ->
                                      !Strings.isEmpty(value)
                                          && !omitIds.contains(value.toUpperCase()))
                              .distinct()
                              .collect(Collectors.toList())
                              .stream()
                              .forEach(
                                  id ->
                                      out.output(
                                          builder
                                              .setHashKey(
                                                  builder.getSpeciesKey()
                                                      + "|"
                                                      + OccurrenceRelationships.normalizeID(id))
                                              .build()));
                        }

                        // hashes for all records
                        if (builder.getDecimalLatitude() != null
                            && builder.getDecimalLongitude() != null
                            && builder.getYear() != null
                            && builder.getMonth() != null
                            && builder.getDay() != null) {
                          out.output(
                              builder
                                  .setHashKey(
                                      String.join(
                                          "|",
                                          builder.getSpeciesKey(),
                                          Long.toString(
                                              Math.round(builder.getDecimalLatitude() * 1000)),
                                          Long.toString(
                                              Math.round(builder.getDecimalLongitude() * 1000)),
                                          Integer.toString(builder.getYear()),
                                          Integer.toString(builder.getMonth()),
                                          Integer.toString(builder.getDay())))
                                  .build());
                        }

                        if (Strings.isNotEmpty(builder.getTaxonKey())
                            && Strings.isNotEmpty(builder.getTypeStatus())) {
                          out.output(
                              builder
                                  .setHashKey(builder.getTaxonKey() + "|" + builder.getTypeStatus())
                                  .build());
                        }

                        if (Strings.isNotEmpty(builder.getTaxonKey())
                            && builder.getYear() != null
                            && Strings.isNotEmpty(builder.getRecordedBy())) {
                          out.output(
                              builder
                                  .setHashKey(
                                      builder.getTaxonKey()
                                          + "|"
                                          + builder.getYear()
                                          + "|"
                                          + builder.getRecordedBy())
                                  .build());
                        }
                      }
                    }))
            .apply(Distinct.create());

    // convert to hashkey -> OccurrenceHash
    PCollection<ClusteringCandidates> candidates =
        hashAll
            .apply(
                MapElements.via(
                    new SimpleFunction<OccurrenceFeatures, KV<String, OccurrenceFeatures>>() {
                      @Override
                      public KV<String, OccurrenceFeatures> apply(OccurrenceFeatures input) {
                        return KV.of(input.getHashKey(), input);
                      }
                    }))
            .apply(GroupByKey.<String, OccurrenceFeatures>create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<OccurrenceFeatures>>, ClusteringCandidates>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, Iterable<OccurrenceFeatures>> source,
                          OutputReceiver<ClusteringCandidates> out) {

                        List<OccurrenceFeatures> result = new ArrayList<>();
                        source.getValue().iterator().forEachRemaining(result::add);

                        if (result.size() > 1) {
                          out.output(
                              ClusteringCandidates.newBuilder()
                                  .setHashKey(source.getKey())
                                  .setCandidates(result)
                                  .build());
                        }
                      }
                    }));

    // need to Group by UUID
    PCollection<KV<String, Relationship>> relationships =
        candidates.apply(
            ParDo.of(
                new DoFn<ClusteringCandidates, KV<String, Relationship>>() {
                  @ProcessElement
                  public void processElement(
                      @Element ClusteringCandidates source,
                      OutputReceiver<KV<String, Relationship>> out) {

                    List<OccurrenceFeatures> candidates = source.getCandidates();
                    while (!candidates.isEmpty()) {

                      OccurrenceFeatures o1 = candidates.remove(0);

                      for (OccurrenceFeatures o2 : candidates) {
                        // if datasetKey != datasetKey
                        if (!o1.getDatasetKey().equals(o2.getDatasetKey())) {

                          RelationshipAssertion assertion =
                              OccurrenceRelationships.generate(o1, o2);
                          if (assertion != null) {

                            Relationship relationship =
                                Relationship.newBuilder()
                                    .setId1(o1.getId())
                                    .setId2(o2.getId())
                                    .setDataset1(o1.getDatasetKey())
                                    .setDataset2(o2.getDatasetKey())
                                    .setReasons(assertion.getJustificationAsDelimited())
                                    .build();

                            // output the relationship in both directions
                            out.output(KV.of(o1.getId(), relationship));
                            out.output(KV.of(o2.getId(), relationship));
                          }
                        }
                      }
                    }
                  }
                }));

    PCollection<Relationships> relationshipsGrouped =
        relationships
            .apply(GroupByKey.<String, Relationship>create())
            .apply(
                MapElements.via(
                    new SimpleFunction<KV<String, Iterable<Relationship>>, Relationships>() {
                      @Override
                      public Relationships apply(KV<String, Iterable<Relationship>> input) {
                        List<Relationship> list = new ArrayList<Relationship>();
                        input.getValue().iterator().forEachRemaining(list::add);
                        return Relationships.newBuilder()
                            .setId(input.getKey())
                            .setRelationships(list)
                            .build();
                      }
                    }));

    // write out to AVRO for debug
    relationshipsGrouped.apply(
        AvroIO.write(Relationships.class)
            .to(options.getClusteringPath() + "/relationships")
            .withSuffix(".avro")
            .withCodec(BASE_CODEC));

    // write candidates out to disk ??
    pipeline.run().waitUntilFinish();
  }

  private static void clearPreviousClustering(ClusteringPipelineOptions options) {
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());
    ALAFsUtils.deleteIfExist(fs, options.getClusteringPath());
  }

  private static PCollection<IndexRecord> loadIndexRecords(
      AllDatasetsPipelinesOptions options, Pipeline p) {
    if (options.getDatasetId() != null && !"all".equalsIgnoreCase(options.getDatasetId())) {
      return p.apply(
          AvroIO.read(IndexRecord.class)
              .from(
                  String.join(
                      "/",
                      options.getAllDatasetsInputPath(),
                      "index-record",
                      options.getDatasetId() + "/*.avro")));
    }

    return p.apply(
        AvroIO.read(IndexRecord.class)
            .from(String.join("/", options.getAllDatasetsInputPath(), "index-record", "*/*.avro")));
  }
}
