package au.org.ala.pipelines.beam;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.ALAPipelinesConfigFactory;
import au.org.ala.kvs.cache.ALAAttributionKVStoreFactory;
import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.common.ALARecordTypes;
import au.org.ala.pipelines.options.UUIDPipelineOptions;
import au.org.ala.pipelines.util.VersionInfo;
import au.org.ala.utils.CombinedYamlConfiguration;
import au.org.ala.utils.ValidationResult;
import au.org.ala.utils.ValidationUtils;
import java.util.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import org.slf4j.MDC;

/**
 * Pipeline responsible for minting UUIDs on new records and rematching existing UUIDs to records
 * that have been previously loaded.
 *
 * <p>This works by:
 *
 * <p>1. Creating a map from ExtendedRecord of UniqueKey -> ExtendedRecord.getId(). The UniqueKey is
 * constructed using the unique terms specified in the collectory.
 *
 * <p>2. Creating a map from source of UniqueKey -> UUID from previous ingestions (this will be
 * blank for newly imported datasets)
 *
 * <p>3. Joining the two maps by the UniqueKey.
 *
 * <p>4. Writing out ALAUUIDRecords containing ExtendedRecord.getId() -> (UUID, UniqueKey). These
 * records are then used in SOLR index generation.
 *
 * <p>5. Backing up previous AVRO ALAUUIDRecords.
 *
 * <p>The end result is a AVRO export ALAUUIDRecords which are then used as a mandatory extension in
 * the generation of the SOLR index.
 *
 * @see ALAUUIDRecord
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ALAUUIDMintingPipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static final String NO_ID_MARKER = "NO_ID";
  public static final String REMOVED_PREFIX_MARKER = "REMOVED_";
  public static final String IDENTIFIERS_DIR = "identifiers";
  public static final String UNIQUE_COMPOSITE_KEY_JOIN_CHAR = "|";

  public static void main(String[] args) throws Exception {
    VersionInfo.print();
    String[] combinedArgs = new CombinedYamlConfiguration(args).toArgs("general", "uuid");
    UUIDPipelineOptions options =
        PipelinesOptionsFactory.create(UUIDPipelineOptions.class, combinedArgs);
    options.setMetaFileName(ValidationUtils.UUID_METRICS);
    MDC.put("datasetId", options.getDatasetId());
    MDC.put("attempt", options.getAttempt().toString());
    MDC.put("step", "UUID");
    PipelinesOptionsFactory.registerHdfs(options);
    run(options);
    // FIXME: Issue logged here: https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/105
    System.exit(0);
  }

  public static void run(UUIDPipelineOptions options) throws Exception {

    // delete metrics if it exists
    MetricsHandler.deleteMetricsFile(options);

    // run the validation pipeline
    log.info("Running validation pipeline");
    ALAUUIDValidationPipeline.run(options);

    // check validation results
    ValidationResult validatioResult = ValidationUtils.checkValidationFile(options);

    log.info("Validation result: {} ", validatioResult.getMessage());

    if (!validatioResult.getValid()) {
      log.error(
          "Unable to run UUID pipeline. Please check validation file: "
              + ValidationUtils.getValidationFilePath(options));
      return;
    }

    Pipeline p = Pipeline.create(options);

    ALAPipelinesConfig config =
        ALAPipelinesConfigFactory.getInstance(
                options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getProperties())
            .get();

    // build the directory path for existing identifiers
    String alaRecordDirectoryPath =
        String.join(
            "/",
            options.getTargetPath(),
            options.getDatasetId().trim(),
            options.getAttempt().toString(),
            IDENTIFIERS_DIR,
            ALARecordTypes.ALA_UUID.name().toLowerCase());
    log.info("Output path {}", alaRecordDirectoryPath);

    // create key value store for data resource metadata
    KeyValueStore<String, ALACollectoryMetadata> dataResourceKvStore =
        ALAAttributionKVStoreFactory.create(config);

    // lookup collectory metadata for this data resource
    ALACollectoryMetadata collectoryMetadata = dataResourceKvStore.get(options.getDatasetId());
    if (collectoryMetadata.equals(ALACollectoryMetadata.EMPTY)) {
      log.error("Unable to retrieve dataset metadata for dataset: " + options.getDatasetId());
      System.exit(1);
    }

    // construct unique list of darwin core terms
    List<String> uniqueTerms = collectoryMetadata.getConnectionParameters().getTermsForUniqueKey();

    if ((uniqueTerms == null || uniqueTerms.isEmpty())) {
      log.error(
          "Unable to proceed, No unique terms specified for dataset: " + options.getDatasetId());
      System.exit(1);
    }

    final List<Term> uniqueDwcTerms = new ArrayList<Term>();
    for (String uniqueTerm : uniqueTerms) {
      Optional<DwcTerm> dwcTerm = getDwcTerm(uniqueTerm);
      if (dwcTerm.isPresent()) {
        uniqueDwcTerms.add(dwcTerm.get());
      } else {
        // create a UnknownTerm for non DWC fields
        uniqueDwcTerms.add(UnknownTerm.build(uniqueTerm.trim()));
      }
    }

    final String datasetID = options.getDatasetId();

    log.info(
        "Transform 1: ExtendedRecord er ->  <uniqueKey, er.getId()> - this generates the UniqueKey.....");
    PCollection<KV<String, String>> extendedRecords =
        p.apply(
                AvroIO.read(ExtendedRecord.class)
                    .from(
                        String.join(
                            "/",
                            options.getTargetPath(),
                            options.getDatasetId().trim(),
                            options.getAttempt().toString(),
                            "interpreted",
                            "verbatim",
                            "*.avro")))
            .apply(
                ParDo.of(
                    new DoFn<ExtendedRecord, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(
                          @Element ExtendedRecord source,
                          OutputReceiver<KV<String, String>> out,
                          ProcessContext c) {
                        out.output(
                            KV.of(
                                ValidationUtils.generateUniqueKey(
                                    datasetID, source, uniqueDwcTerms),
                                source.getId()));
                      }
                    }));

    PCollection<KV<String, String>> alaUuids;

    log.info("Transform 2: ALAUUIDRecord ur ->  <uniqueKey, uuid> (assume incomplete)");
    FileSystem fs =
        FsUtils.getFileSystem(
            options.getHdfsSiteConfig(), options.getCoreSiteConfig(), options.getInputPath());
    Path path = new Path(alaRecordDirectoryPath);

    if (fs.exists(path)) {
      alaUuids =
          p.apply(AvroIO.read(ALAUUIDRecord.class).from(alaRecordDirectoryPath + "/*.avro"))
              .apply(ParDo.of(new ALAUUIDRecordKVFcn()));
    } else {

      TypeDescriptor<KV<String, String>> td = new TypeDescriptor<KV<String, String>>() {};
      log.warn(
          "Previous ALAUUIDRecord records where not found. This is expected for new datasets, but is a problem "
              + "for previously loaded datasets - will mint new ones......");
      alaUuids = p.apply(Create.empty(td));
    }

    log.info("Create join collection");
    PCollection<KV<String, KV<String, String>>> joinedPcollection =
        org.apache.beam.sdk.extensions.joinlibrary.Join.fullOuterJoin(
            extendedRecords, alaUuids, NO_ID_MARKER, NO_ID_MARKER);

    log.info("Create ALAUUIDRecords and write out to AVRO");
    joinedPcollection
        .apply(ParDo.of(new CreateALAUUIDRecordFcn()))
        .apply(
            AvroIO.write(ALAUUIDRecord.class)
                .to(alaRecordDirectoryPath + "_new/interpret")
                .withSuffix(".avro")
                .withCodec(BASE_CODEC));

    log.info("Running the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();

    Path existingVersionUUids = new Path(alaRecordDirectoryPath);
    Path newVersionUUids = new Path(alaRecordDirectoryPath + "_new");

    // rename backup existing
    if (fs.exists(existingVersionUUids)) {
      String backupPath = alaRecordDirectoryPath + "_backup_" + System.currentTimeMillis();
      log.info("Backing up existing UUIDs to {}", backupPath);
      fs.rename(existingVersionUUids, new Path(backupPath));
    }
    // rename new version to current path
    fs.rename(newVersionUUids, existingVersionUUids);
    log.info("Pipeline complete.");

    log.info("Writing metrics.....");
    MetricsHandler.saveCountersToTargetPathFile(options, result.metrics());
    log.info("Writing metrics written.");
  }

  /** Function to create ALAUUIDRecords. */
  static class CreateALAUUIDRecordFcn extends DoFn<KV<String, KV<String, String>>, ALAUUIDRecord> {

    // TODO this counts are inaccurate when using SparkRunner
    private final Counter orphanedUniqueKeys =
        Metrics.counter(CreateALAUUIDRecordFcn.class, "orphanedUniqueKeys");
    private final Counter newUuids = Metrics.counter(CreateALAUUIDRecordFcn.class, "newUuids");
    private final Counter preservedUuids =
        Metrics.counter(CreateALAUUIDRecordFcn.class, "preservedUuids");

    @ProcessElement
    public void processElement(
        @Element KV<String, KV<String, String>> uniqueKeyMap, OutputReceiver<ALAUUIDRecord> out) {

      // get the matched ExtendedRecord.getId()
      String id = uniqueKeyMap.getValue().getKey();

      // get the UUID
      String uuid = uniqueKeyMap.getValue().getValue();

      // get the constructed key
      String uniqueKey = uniqueKeyMap.getKey();

      // if UUID == NO_ID_MARKER, we have a new record so we need a new UUID.
      if (Objects.equals(uuid, NO_ID_MARKER)) {
        newUuids.inc();
        uuid = UUID.randomUUID().toString();
      } else if (Objects.equals(id, NO_ID_MARKER)) {
        id = REMOVED_PREFIX_MARKER + UUID.randomUUID().toString();
        orphanedUniqueKeys.inc();
      } else {
        preservedUuids.inc();
      }

      ALAUUIDRecord aur =
          ALAUUIDRecord.newBuilder().setUniqueKey(uniqueKey).setUuid(uuid).setId(id).build();
      out.output(aur);
    }
  }

  /** Transform to create a map of unique keys built from previous runs and UUID. */
  static class ALAUUIDRecordKVFcn extends DoFn<ALAUUIDRecord, KV<String, String>> {
    @ProcessElement
    public void processElement(
        @Element ALAUUIDRecord alaUUIDRecord, OutputReceiver<KV<String, String>> out) {
      out.output(KV.of(alaUUIDRecord.getUniqueKey(), alaUUIDRecord.getUuid()));
    }
  }

  /**
   * Match the darwin core term which has been supplied in simple camel case format e.g.
   * catalogNumber.
   */
  static Optional<DwcTerm> getDwcTerm(String name) {
    try {
      return Optional.of(DwcTerm.valueOf(name));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
