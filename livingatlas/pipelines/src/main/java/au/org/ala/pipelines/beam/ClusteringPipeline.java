package au.org.ala.pipelines.beam;

import au.org.ala.clustering.OccurrenceRelationships;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.ClusteringPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
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
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.ClusteringCandidate;
import org.gbif.pipelines.io.avro.ClusteringCandidates;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.OccurrenceHash;
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

    // read index records
    PCollection<IndexRecord> indexRecords = loadIndexRecords(options, pipeline);

    // create hashes for everything
    PCollection<OccurrenceHash> hashAll =
        indexRecords
            .apply(
                ParDo.of(
                    new DoFn<IndexRecord, OccurrenceHash>() {
                      @ProcessElement
                      public void processElement(
                          @Element IndexRecord source, OutputReceiver<OccurrenceHash> out) {

                        String uuid = source.getId();

                        String speciesKey = source.getStrings().get("speciesID");
                        String taxonKey = source.getStrings().get("taxonID");
                        String datasetKey = source.getStrings().get("dataResourceUid");
                        String basisOfRecord = source.getStrings().get("basisOfRecord");

                        Double lat = source.getDoubles().get("decimalLatitude");
                        Double lng = source.getDoubles().get("decimalLongitude");
                        Integer year = source.getInts().get("year");
                        Integer month = source.getInts().get("month");
                        Integer day = source.getInts().get("day");

                        String typeStatus = source.getStrings().get("typeStatus");
                        String recordedBy = source.getStrings().get("recordedBy");

                        // specimen only hashes
                        if (Strings.isNotEmpty(speciesKey)
                            && basisOfRecord != null
                            && specimenBORs.contains(basisOfRecord)) {
                          // output hashes for each combination
                          Arrays.asList(
                                  source.getStrings().get("occurrenceID"),
                                  source.getStrings().get("fieldNumber"),
                                  source.getStrings().get("recordNumber"),
                                  source.getStrings().get("catalogNumber"),
                                  source.getStrings().get("otherCatalogNumbers"))
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
                                          OccurrenceHash.newBuilder()
                                              .setHashKey(
                                                  speciesKey
                                                      + "|"
                                                      + OccurrenceRelationships.normalizeID(id))
                                              .setDatasetKey(datasetKey)
                                              .setUuid(uuid)
                                              .build()));
                        }

                        if (lat != null
                            && lng != null
                            && year != null
                            && month != null
                            && day != null) {
                          out.output(
                              OccurrenceHash.newBuilder()
                                  .setHashKey(
                                      speciesKey
                                          + "|"
                                          + Math.round(lat * 1000)
                                          + "|"
                                          + Math.round(lng * 1000)
                                          + "|"
                                          + year
                                          + "|"
                                          + month
                                          + "|"
                                          + day)
                                  .setDatasetKey(datasetKey)
                                  .setUuid(uuid)
                                  .build());
                        }

                        if (Strings.isNotEmpty(taxonKey) && Strings.isNotEmpty(typeStatus)) {
                          out.output(
                              OccurrenceHash.newBuilder()
                                  .setHashKey(taxonKey + "|" + typeStatus)
                                  .setDatasetKey(datasetKey)
                                  .setUuid(uuid)
                                  .build());
                        }

                        if (Strings.isNotEmpty(taxonKey)
                            && year != null
                            && Strings.isNotEmpty(recordedBy)) {
                          out.output(
                              OccurrenceHash.newBuilder()
                                  .setHashKey(taxonKey + "|" + year + "|" + recordedBy)
                                  .setDatasetKey(datasetKey)
                                  .setUuid(uuid)
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
                    new SimpleFunction<OccurrenceHash, KV<String, OccurrenceHash>>() {
                      @Override
                      public KV<String, OccurrenceHash> apply(OccurrenceHash input) {
                        return KV.of(input.getHashKey(), input);
                      }
                    }))
            .apply(GroupByKey.<String, OccurrenceHash>create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<OccurrenceHash>>, ClusteringCandidates>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, Iterable<OccurrenceHash>> source,
                          OutputReceiver<ClusteringCandidates> out) {

                        List<ClusteringCandidate> result = new ArrayList<>();
                        source
                            .getValue()
                            .iterator()
                            .forEachRemaining(
                                new Consumer<OccurrenceHash>() {
                                  @Override
                                  public void accept(OccurrenceHash occurrenceHash) {
                                    result.add(
                                        ClusteringCandidate.newBuilder()
                                            .setUuid(occurrenceHash.getUuid())
                                            .setDatasetKey(occurrenceHash.getDatasetKey())
                                            .build());
                                  }
                                });

/*
      SELECT t1.gbifId as id1, t1.datasetKey as ds1, t2.gbifId as id2, t2.datasetKey as ds2
      FROM DF_hashed t1 JOIN DF_hashed t2 ON t1.hash = t2.hash
      WHERE
        t1.gbifId < t2.gbifId AND
        t1.datasetKey != t2.datasetKey
      GROUP BY t1.gbifId, t1.datasetKey, t2.gbifId, t2.datasetKey
 */




                        if (result.size() > 1) {
                          out.output(
                              ClusteringCandidates.newBuilder()
                                  .setHashKey(source.getKey())
                                  .setCandidates(result)
                                  .build());
                        }
                      }
                    }));

    // write out to AVRO for debug
    if (options.getDumpCandidatesForDebug()) {
      candidates.apply(
              AvroIO.write(ClusteringCandidates.class)
                      .to(options.getClusteringPath() + "/candidates")
                      .withSuffix(".avro")
                      .withCodec(BASE_CODEC));
    }

    //we've grouped candidates




    // write candidates out to disk ??
    pipeline.run().waitUntilFinish();
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
