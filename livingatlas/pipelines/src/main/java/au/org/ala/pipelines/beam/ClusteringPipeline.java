package au.org.ala.pipelines.beam;

import au.org.ala.clustering.ClusterPair;
import au.org.ala.clustering.ClusteringCandidates;
import au.org.ala.clustering.HashKeyOccurrence;
import au.org.ala.clustering.RepresentativeRecordUtils;
import au.org.ala.pipelines.options.AllDatasetsPipelinesOptions;
import au.org.ala.pipelines.options.ClusteringPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.FileNotFoundException;
import java.util.*;
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
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships;
import org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.slf4j.MDC;

/**
 * Clustering pipeline which is a Apache Beam port of the work in the module
 * https://github.com/gbif/occurrence/tree/master/occurrence-clustering
 *
 * <p>This pipeline required that IndexRecords have been generated in a prior step for all datasets
 * (see @{@link IndexRecordPipeline}.
 *
 * <p>The IndexRecords which are stored on the filesystem in AVRO format are read and used to
 * generate clusters using the algorithm from the occurrence-clustering module.
 *
 * <p>The output is then write to AVRO files using the @{@link Relationships} AVRO class.
 */
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

    MDC.put("datasetId", "ALL_RECORDS");
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
    PCollection<HashKeyOccurrence> hashAll =
        indexRecords
            .apply(
                ParDo.of(
                    new DoFn<IndexRecord, HashKeyOccurrence>() {
                      @ProcessElement
                      public void processElement(
                          @Element IndexRecord source, OutputReceiver<HashKeyOccurrence> out) {

                        String datasetKey = source.getStrings().get("dataResourceUid");
                        if (datasetKey == null) {
                          log.error("datasetKey null for record " + source.getId());
                          return;
                        }

                        String speciesKey = source.getStrings().get("speciesID");
                        String taxonKey = source.getStrings().get("taxonConceptOD");
                        String typeStatus = source.getStrings().get("typeStatus");
                        String basisOfRecord = source.getStrings().get("basisOfRecord");
                        Double decimalLatitude = source.getDoubles().get("decimalLatitude");
                        Double decimalLongitude = source.getDoubles().get("decimalLongitude");

                        Integer year = source.getInts().get("year");
                        Integer month = source.getInts().get("month");
                        Integer day = source.getInts().get("day");

                        String occurrenceID = source.getStrings().get("occurrenceID");
                        String fieldNumber = source.getStrings().get("fieldNumber");
                        String recordNumber = source.getStrings().get("recordNumber");
                        String catalogNumber = source.getStrings().get("catalogNumber");
                        String otherCatalogNumbers = source.getStrings().get("otherCatalogNumbers");

                        String recordedBy = source.getStrings().get("recordedBy");

                        HashKeyOccurrence.HashKeyOccurrenceBuilder builder =
                            HashKeyOccurrence.builder()
                                .id(source.getId())
                                .datasetKey(datasetKey)
                                .speciesKey(speciesKey)
                                .taxonKey(source.getStrings().get("taxonConceptID"))
                                .basisOfRecord(source.getStrings().get("basisOfRecord"))
                                .decimalLatitude(source.getDoubles().get("decimalLatitude"))
                                .decimalLongitude(source.getDoubles().get("decimalLongitude"))
                                .year(source.getInts().get("year"))
                                .month(source.getInts().get("month"))
                                .day(source.getInts().get("day"))
                                .eventDate(source.getLongs().get("eventDate").toString())
                                .typeStatus(source.getStrings().get("typeStatus"))
                                .recordedBy(source.getStrings().get("recordedBy"))
                                .recordedBy(source.getStrings().get("fieldNumber"))
                                .recordNumber(source.getStrings().get("recordNumber"))
                                .catalogNumber(source.getStrings().get("catalogNumber"))
                                .occurrenceID(source.getStrings().get("occurrenceID"))
                                .otherCatalogNumbers(
                                    source.getStrings().get("otherCatalogNumbers"));
                        //
                        // specimen only hashes
                        if (Strings.isNotEmpty(speciesKey)
                            && Strings.isNotEmpty(basisOfRecord)
                            && specimenBORs.contains(basisOfRecord)) {

                          // output hashes for each combination
                          Arrays.asList(
                                  occurrenceID,
                                  fieldNumber,
                                  recordNumber,
                                  catalogNumber,
                                  otherCatalogNumbers)
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
                                              .hashKey(
                                                  speciesKey
                                                      + "|"
                                                      + OccurrenceRelationships.normalizeID(id))
                                              .build()));
                        }

                        // hashes for all records
                        if (decimalLatitude != null
                            && decimalLongitude != null
                            && year != null
                            && month != null
                            && day != null) {
                          out.output(
                              builder
                                  .hashKey(
                                      String.join(
                                          "|",
                                          speciesKey,
                                          Long.toString(Math.round(decimalLatitude * 1000)),
                                          Long.toString(Math.round(decimalLongitude * 1000)),
                                          Integer.toString(year),
                                          Integer.toString(month),
                                          Integer.toString(day)))
                                  .build());
                        }

                        if (Strings.isNotEmpty(taxonKey) && Strings.isNotEmpty(typeStatus)) {
                          out.output(builder.hashKey(taxonKey + "|" + typeStatus).build());
                        }

                        if (Strings.isNotEmpty(taxonKey)
                            && year != null
                            && Strings.isNotEmpty(recordedBy)) {
                          out.output(
                              builder.hashKey(taxonKey + "|" + year + "|" + recordedBy).build());
                        }
                      }
                    }))
            .apply(Distinct.create());

    // convert to hashkey -> OccurrenceHash
    PCollection<ClusteringCandidates> candidates =
        hashAll
            .apply(
                MapElements.via(
                    new SimpleFunction<HashKeyOccurrence, KV<String, HashKeyOccurrence>>() {
                      @Override
                      public KV<String, HashKeyOccurrence> apply(HashKeyOccurrence input) {
                        return KV.of(input.getHashKey(), input);
                      }
                    }))
            .apply(GroupByKey.<String, HashKeyOccurrence>create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<HashKeyOccurrence>>, ClusteringCandidates>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, Iterable<OccurrenceFeatures>> source,
                          OutputReceiver<ClusteringCandidates> out) {

                        List<OccurrenceFeatures> result = new ArrayList<>();
                        source.getValue().iterator().forEachRemaining(result::add);

                        if (result.size() > 1) {
                          out.output(
                              ClusteringCandidates.builder()
                                  .hashKey(source.getKey())
                                  .candidates(result)
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

                    if (source.getCandidates().size() < 50) {

                      List<OccurrenceFeatures> candidates = source.getCandidates();
                      List<ClusterPair> pairs = new ArrayList<>();

                      while (!candidates.isEmpty()) {

                        OccurrenceFeatures o1 = candidates.remove(0);

                        for (OccurrenceFeatures o2 : candidates) {

                          RelationshipAssertion<OccurrenceFeatures> assertion =
                              OccurrenceRelationships.generate(o1, o2);

                          if (assertion != null) {
                            pairs.add(
                                ClusterPair.builder().o1(o1).o2(o2).assertion(assertion).build());
                          }
                          //                        }
                        }
                      }

                      if (pairs.size() > 10) {
                        log.error("Finding clusters of size: " + pairs.size());
                      }

                      // cluster occurrences
                      List<List<OccurrenceFeatures>> clusters =
                          RepresentativeRecordUtils.createClusters(pairs);

                      if (clusters.size() > 1) {
                        log.error("Finding no of clusters of size: " + clusters.size());
                      }

                      // within each cluster, nominate the
                      // RepresentativeRecord (primary) and the AssociatedRecord (duplicate)
                      for (List<OccurrenceFeatures> cluster : clusters) {

                        if (cluster.size() < 50) {

                          // find the representative record
                          OccurrenceFeatures representativeRecord =
                              RepresentativeRecordUtils.findRepresentativeRecord(cluster);

                          // determine representative records
                          Relationship.Builder builder =
                              Relationship.newBuilder()
                                  .setRepId(representativeRecord.getId())
                                  .setRepDataset(representativeRecord.getDatasetKey());

                          for (OccurrenceFeatures associatedRecord : cluster) {

                            if (!associatedRecord.getId().equals(representativeRecord.getId())) {

                              // determine representative records
                              RelationshipAssertion assertion =
                                  OccurrenceRelationships.generate(
                                      representativeRecord, associatedRecord);

                              if (assertion != null) {
                                Relationship r =
                                    builder
                                        .setDupId(associatedRecord.getId())
                                        .setDupDataset(associatedRecord.getDatasetKey())
                                        .setJustification(assertion.getJustificationAsDelimited())
                                        .build();

                                out.output(KV.of(representativeRecord.getId(), r));
                                out.output(KV.of(associatedRecord.getId(), r));
                              } else {

                                // do we go back for the ClusterPair ???
                                // and work out which is the representative between the pair ??
                                Optional<ClusterPair> clusterPair =
                                    pairs.stream()
                                        .filter(
                                            pair ->
                                                pair.getO1().equals(associatedRecord)
                                                    || pair.getO2().equals(associatedRecord))
                                        .findFirst();

                                if (clusterPair.isPresent()) {
                                  // which is the representative ?
                                  OccurrenceFeatures rep =
                                      RepresentativeRecordUtils.pickRepresentative(
                                          Arrays.asList(
                                              clusterPair.get().getO1(),
                                              clusterPair.get().getO2()));

                                  OccurrenceFeatures dup =
                                      clusterPair.get().getO1().equals(rep)
                                          ? clusterPair.get().getO2()
                                          : clusterPair.get().getO1();

                                  Relationship r =
                                      builder
                                          .setRepId(rep.getId())
                                          .setRepDataset(rep.getDatasetKey())
                                          .setDupId(dup.getId())
                                          .setDupDataset(dup.getDatasetKey())
                                          .setJustification(
                                              clusterPair
                                                  .get()
                                                  .getAssertion()
                                                  .getJustificationAsDelimited())
                                          .build();

                                  out.output(KV.of(rep.getId(), r));
                                  out.output(KV.of(dup.getId(), r));
                                }
                              }
                            }
                          }
                        } else {
                          log.warn("Avoiding marking a cluster of size {}", cluster.size());
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
            .to(options.getClusteringPath() + "/relationships/relationships")
            .withSuffix(".avro")
            .withCodec(BASE_CODEC));

    // write candidates out to disk ??
    pipeline.run().waitUntilFinish();
  }

  private static void clearPreviousClustering(ClusteringPipelineOptions options) {

    log.info("Clearing clustering path {}", options.getClusteringPath());
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());
    ALAFsUtils.deleteIfExist(fs, options.getClusteringPath() + "/relationships");
    log.info("Cleared clustering path {}.", options.getClusteringPath());
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
