package au.org.ala.pipelines.beam;

import au.org.ala.clustering.*;
import au.org.ala.pipelines.options.ClusteringPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.ALAFsUtils;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationUtils;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships;
import org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.jetbrains.annotations.NotNull;
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
 * <p>The output is then written to AVRO files using the @{@link Relationships} AVRO class.
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

  public static void main(String[] args) throws IOException {
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
    PCollection<IndexRecord> indexRecords = ALAFsUtils.loadIndexRecords(options, pipeline);

    final Integer candidatesCutoff = options.getCandidatesCutoff();

    // create hashes for everything
    PCollection<HashKeyOccurrence> hashAll =
        indexRecords.apply(
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
                    String taxonKey = source.getStrings().get(DwcTerm.taxonConceptID.simpleName());
                    String scientificName =
                        source.getStrings().get(DwcTerm.scientificName.simpleName());
                    List<String> typeStatus =
                        source.getMultiValues().get(DwcTerm.typeStatus.simpleName());
                    String basisOfRecord =
                        source.getStrings().get(DwcTerm.basisOfRecord.simpleName());
                    Double decimalLatitude =
                        source.getDoubles().get(DwcTerm.decimalLatitude.simpleName());
                    Double decimalLongitude =
                        source.getDoubles().get(DwcTerm.decimalLongitude.simpleName());
                    String countryCode = source.getStrings().get(DwcTerm.countryCode.simpleName());

                    Integer year = source.getInts().get(DwcTerm.year.simpleName());
                    Integer month = source.getInts().get(DwcTerm.month.simpleName());
                    Integer day = source.getInts().get(DwcTerm.day.simpleName());

                    String occurrenceID =
                        source.getStrings().get(DwcTerm.occurrenceID.simpleName());
                    String fieldNumber = source.getStrings().get(DwcTerm.fieldNumber.simpleName());
                    String recordNumber =
                        source.getStrings().get(DwcTerm.recordNumber.simpleName());
                    String catalogNumber =
                        source.getStrings().get(DwcTerm.catalogNumber.simpleName());
                    List<String> otherCatalogNumbers =
                        source.getMultiValues().get(DwcTerm.otherCatalogNumbers.simpleName());

                    List<String> recordedBy =
                        source.getMultiValues().get(DwcTerm.recordedBy.simpleName());

                    Long eventDateL = source.getLongs().get(DwcTerm.eventDate.simpleName());
                    String eventDate = "";
                    if (eventDateL != null) {
                      eventDate = eventDateL.toString();
                    }

                    HashKeyOccurrenceBuilder builder =
                        HashKeyOccurrenceBuilder.aHashKeyOccurrence()
                            .withId(source.getId())
                            .withDatasetKey(datasetKey)
                            .withSpeciesKey(speciesKey)
                            .withScientificName(scientificName)
                            .withCountryCode(countryCode)
                            .withTaxonKey(taxonKey)
                            .withBasisOfRecord(basisOfRecord)
                            .withDecimalLatitude(decimalLatitude)
                            .withDecimalLongitude(decimalLongitude)
                            .withYear(year)
                            .withMonth(month)
                            .withDay(day)
                            .withEventDate(eventDate)
                            .withTypeStatus(typeStatus)
                            .withRecordedBy(recordedBy)
                            .withFieldNumber(fieldNumber)
                            .withRecordNumber(recordNumber)
                            .withCatalogNumber(catalogNumber)
                            .withOccurrenceID(occurrenceID)
                            .withOtherCatalogNumbers(otherCatalogNumbers);

                    // specimen only hashes
                    if (Strings.isNotEmpty(speciesKey)
                        && Strings.isNotEmpty(basisOfRecord)
                        && specimenBORs.contains(basisOfRecord)) {

                      Stream<String> ids =
                          otherCatalogNumbers == null
                              ? Stream.of(occurrenceID, fieldNumber, recordNumber, catalogNumber)
                              : Stream.concat(
                                  Stream.of(occurrenceID, fieldNumber, recordNumber, catalogNumber),
                                  otherCatalogNumbers.stream());

                      // output hashes for each combination
                      ids.filter(
                              value ->
                                  !Strings.isEmpty(value) && !omitIds.contains(value.toUpperCase()))
                          .distinct()
                          .collect(Collectors.toList())
                          .forEach(
                              id ->
                                  out.output(
                                      builder
                                          .withHashKey(
                                              speciesKey
                                                  + "|"
                                                  + OccurrenceRelationships.normalizeID(id))
                                          .build()));
                    }

                    // hashkeys for all records
                    // 1. speciesKey|decimalLatitude|decimalLongitude|year|month|day
                    if (decimalLatitude != null
                        && decimalLongitude != null
                        && year != null
                        && month != null
                        && day != null
                        && speciesKey != null) {
                      out.output(
                          builder
                              .withHashKey(
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

                    // 2. type status hashkeys
                    if (Strings.isNotEmpty(taxonKey) && typeStatus != null) {
                      for (String t : typeStatus) {
                        out.output(builder.withHashKey(taxonKey + "|" + t).build());
                      }
                    }

                    // 3. taxonKey|year|recordedBy hashkeys
                    if (Strings.isNotEmpty(taxonKey) && year != null && recordedBy != null) {
                      for (String r : recordedBy) {
                        out.output(builder.withHashKey(taxonKey + "|" + year + "|" + r).build());
                      }
                    }
                  }
                }));

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
            .apply(GroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, Iterable<HashKeyOccurrence>>, ClusteringCandidates>() {
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, Iterable<HashKeyOccurrence>> source,
                          OutputReceiver<ClusteringCandidates> out) {

                        List<HashKeyOccurrence> result = new ArrayList<>();
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

                    log.info("Candidates: {}", source.getCandidates().size());
                    if (source.getCandidates().size() < candidatesCutoff) {
                      List<KV<String, Relationship>> output =
                          createRelationships(source, candidatesCutoff);
                      log.info(
                          "Candidates: {}, Relationships {}",
                          source.getCandidates().size(),
                          output.size());
                      output.forEach(out::output);
                    }
                  }
                }));

    if (options.isOutputDebugAvro()) {
      outputDebugHashKeys(options, hashAll);
      outputDebugCandidates(options, candidates);
      outputDebugRelationships(options, candidatesCutoff, candidates);
      outputDebugRelationshipsUngrouped(options, relationships);
    }

    // group by record ID
    PCollection<Relationships> relationshipsGrouped =
        relationships
            .apply(GroupByKey.create())
            .apply(
                MapElements.via(
                    new SimpleFunction<KV<String, Iterable<Relationship>>, Relationships>() {
                      @Override
                      public Relationships apply(KV<String, Iterable<Relationship>> input) {
                        Set<Relationship> list = new HashSet<>();
                        // we are only support one duplicate relationship
                        list.add(input.getValue().iterator().next());
                        return Relationships.newBuilder()
                            .setId(input.getKey())
                            .setRelationships(new ArrayList<>(list))
                            .build();
                      }
                    }))
            .apply(
                ParDo.of(
                    new DoFn<Relationships, Relationships>() {
                      @ProcessElement
                      public void processElement(
                          @Element Relationships duplicateRelationship,
                          OutputReceiver<Relationships> out) {

                        Relationship r = duplicateRelationship.getRelationships().iterator().next();

                        // create a relationship for the representative record
                        Relationships representativeRelationship =
                            Relationships.newBuilder()
                                .setId(r.getRepId())
                                .setRelationships(duplicateRelationship.getRelationships())
                                .build();

                        out.output(representativeRelationship);
                        out.output(duplicateRelationship);
                      }
                    }))
            .apply(
                MapElements.via(
                    new SimpleFunction<Relationships, KV<String, Relationships>>() {
                      @Override
                      public KV<String, Relationships> apply(Relationships input) {
                        return KV.of(input.getId(), input);
                      }
                    }))
            .apply(GroupByKey.create())
            .apply(
                MapElements.via(
                    new SimpleFunction<KV<String, Iterable<Relationships>>, Relationships>() {
                      @Override
                      public Relationships apply(KV<String, Iterable<Relationships>> input) {

                        Set<Relationship> set = new HashSet<>();
                        for (Relationships rs : input.getValue()) {
                          set.addAll(rs.getRelationships());
                        }
                        return Relationships.newBuilder()
                            .setId(input.getKey())
                            .setRelationships(new ArrayList<>(set))
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

  private static void outputDebugRelationshipsUngrouped(
      ClusteringPipelineOptions options, PCollection<KV<String, Relationship>> relationships) {
    relationships
        .apply(
            MapElements.via(
                new SimpleFunction<KV<String, Relationship>, String>() {
                  @Override
                  public String apply(KV<String, Relationship> input) {
                    return String.join(
                        ",",
                        input.getKey(),
                        input.getValue().getDupId(),
                        input.getValue().getRepId(),
                        input.getValue().getDupDataset(),
                        input.getValue().getRepDataset(),
                        input.getValue().getJustification());
                  }
                }))
        .apply(
            TextIO.write()
                .withoutSharding()
                .to(
                    options.getClusteringPath()
                        + "/relationships-debug/relationships-ungrouped.csv"));
  }

  private static void outputDebugRelationships(
      ClusteringPipelineOptions options,
      Integer candidatesCutoff,
      PCollection<ClusteringCandidates> candidates) {
    candidates
        .apply(
            ParDo.of(
                new DoFn<ClusteringCandidates, String>() {
                  @ProcessElement
                  public void processElement(
                      @Element ClusteringCandidates source, OutputReceiver<String> out) {

                    if (source.getCandidates().size() < candidatesCutoff) {

                      List<HashKeyOccurrence> candidates = source.getCandidates();
                      List<HashKeyOccurrence> processed = new ArrayList<>();

                      for (HashKeyOccurrence o1 : candidates) {

                        processed.add(o1);

                        for (HashKeyOccurrence o2 : candidates) {

                          if (!processed.contains(o2)) {
                            RelationshipAssertion<HashKeyOccurrence> assertion =
                                OccurrenceRelationships.generate(o1, o2);

                            if (assertion != null) {
                              out.output(
                                  o1.getId()
                                      + ","
                                      + o2.getId()
                                      + ","
                                      + assertion.getJustificationAsDelimited());
                            }
                          }
                        }
                      }
                    }
                  }
                }))
        .apply(
            TextIO.write()
                .withoutSharding()
                .to(options.getClusteringPath() + "/relationships-debug/relationships.csv"));
  }

  private static void outputDebugCandidates(
      ClusteringPipelineOptions options, PCollection<ClusteringCandidates> candidates) {
    candidates
        .apply(
            MapElements.via(
                new SimpleFunction<ClusteringCandidates, String>() {
                  @Override
                  public String apply(ClusteringCandidates input) {
                    return input.getHashKey()
                        + ","
                        + input.getCandidates().stream()
                            .map(HashKeyOccurrence::getId)
                            .collect(Collectors.joining("|"));
                  }
                }))
        .apply(
            TextIO.write()
                .withoutSharding()
                .to(options.getClusteringPath() + "/relationships-debug/candidates.csv"));
  }

  private static void outputDebugHashKeys(
      ClusteringPipelineOptions options, PCollection<HashKeyOccurrence> hashAll) {
    hashAll
        .apply(
            MapElements.via(
                new SimpleFunction<HashKeyOccurrence, String>() {
                  @Override
                  public String apply(HashKeyOccurrence input) {
                    return String.join(
                        ",",
                        input.getHashKey(),
                        input.getId(),
                        input.getDatasetKey(),
                        input.getSpeciesKey(),
                        input.getScientificName(),
                        input.getCountryCode(),
                        input.getTaxonKey(),
                        input.getBasisOfRecord(),
                        input.getDecimalLatitude() + "",
                        input.getDecimalLongitude() + "",
                        input.getYear() + "",
                        input.getMonth() + "",
                        input.getDay() + "",
                        input.getEventDate(),
                        String.join("|", input.getTypeStatus()),
                        String.join("|", input.getRecordedBy()),
                        input.getFieldNumber(),
                        input.getRecordNumber(),
                        input.getCatalogNumber(),
                        input.getOccurrenceID(),
                        String.join("|", input.getOtherCatalogNumbers()));
                  }
                }))
        .apply(
            TextIO.write()
                .withoutSharding()
                .to(options.getClusteringPath() + "/relationships-debug/hashkeys.csv"));
  }

  @NotNull
  public static List<KV<String, Relationship>> createRelationships(
      ClusteringCandidates source, Integer candidatesCutoff) {

    List<KV<String, Relationship>> output = new ArrayList<>();
    if (source.getCandidates().size() < candidatesCutoff) {

      List<HashKeyOccurrence> candidates = source.getCandidates();
      List<ClusterPair> pairs = new ArrayList<>();
      List<HashKeyOccurrence> processed = new ArrayList<>();

      for (HashKeyOccurrence o1 : candidates) {

        processed.add(o1);

        for (HashKeyOccurrence o2 : candidates) {

          if (!processed.contains(o2)) {
            RelationshipAssertion<HashKeyOccurrence> assertion =
                OccurrenceRelationships.generate(o1, o2);

            if (assertion != null) {
              pairs.add(ClusterPair.builder().o1(o1).o2(o2).assertion(assertion).build());
            }
          }
        }
      }

      // cluster occurrences
      List<Set<HashKeyOccurrence>> clusters = RepresentativeRecordUtils.createClusters(pairs);

      if (clusters.size() > 1) {
        log.error("Finding no of clusters of size: " + clusters.size());
      }

      // within each cluster, nominate the
      // RepresentativeRecord (primary) and the AssociatedRecord (duplicate)
      for (Set<HashKeyOccurrence> cluster : clusters) {

        if (cluster.size() < candidatesCutoff) {

          // find the representative record
          HashKeyOccurrence representativeRecord =
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
                  OccurrenceRelationships.generate(representativeRecord, associatedRecord);

              if (assertion != null) {
                Relationship r =
                    builder
                        .setDupId(associatedRecord.getId())
                        .setDupDataset(associatedRecord.getDatasetKey())
                        .setJustification(assertion.getJustificationAsDelimited())
                        .build();

                output.add(KV.of(associatedRecord.getId(), r));
              } else {

                // do we go back for the ClusterPair ???
                // and work out which is the representative between the pair
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
                          new HashSet<>(
                              Arrays.asList(clusterPair.get().getO1(), clusterPair.get().getO2())));

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
                              clusterPair.get().getAssertion().getJustificationAsDelimited())
                          .build();

                  output.add(KV.of(dup.getId(), r));
                }
              }
            }
          }
        } else {
          log.warn("Avoiding marking a cluster of size {}", cluster.size());
        }
      }
    }
    return output;
  }

  private static void clearPreviousClustering(ClusteringPipelineOptions options) {

    log.info("Clearing clustering path {}", options.getClusteringPath());
    FileSystem fs =
        FsUtils.getFileSystem(
            HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig()),
            options.getInputPath());
    ALAFsUtils.deleteIfExist(fs, options.getClusteringPath() + "/relationships");
    ALAFsUtils.deleteIfExist(fs, options.getClusteringPath() + "/relationships-debug");
    log.info("Cleared clustering path {}.", options.getClusteringPath());
  }
}
