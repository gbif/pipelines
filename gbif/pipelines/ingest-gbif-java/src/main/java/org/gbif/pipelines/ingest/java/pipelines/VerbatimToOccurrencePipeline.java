package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.IDENTIFIER_ABSENT;
import static org.gbif.pipelines.ingest.java.transforms.InterpretedAvroWriter.createAvroWriter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.ingest.java.pipelines.interpretation.Shutdown;
import org.gbif.pipelines.ingest.java.pipelines.interpretation.TransformsFactory;
import org.gbif.pipelines.ingest.java.transforms.InterpretedAvroReader;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.java.DefaultValuesTransform;
import org.gbif.pipelines.transforms.java.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.java.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.metadata.MetadataTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdAbsentTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.slf4j.MDC;

/**
 * Pipeline sequence:
 *
 * <pre>
 *    1) Reads verbatim.avro file
 *    2) Interprets and converts avro {@link org.gbif.pipelines.io.avro.ExtendedRecord} file to:
 *      {@link org.gbif.pipelines.io.avro.MetadataRecord},
 *      {@link org.gbif.pipelines.io.avro.BasicRecord},
 *      {@link org.gbif.pipelines.io.avro.TemporalRecord},
 *      {@link org.gbif.pipelines.io.avro.MultimediaRecord},
 *      {@link org.gbif.pipelines.io.avro.ImageRecord},
 *      {@link org.gbif.pipelines.io.avro.AudubonRecord},
 *      {@link org.gbif.pipelines.io.avro.MeasurementOrFactRecord},
 *      {@link org.gbif.pipelines.io.avro.TaxonRecord},
 *      {@link org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord},
 *      {@link org.gbif.pipelines.io.avro.LocationRecord}
 *    3) Writes data to independent files
 * </pre>
 *
 * <p>How to run:
 *
 * <pre>{@code
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline some.properties
 *
 * or pass all parameters:
 *
 * java -cp target/ingest-gbif-java-BUILD_VERSION-shaded.jar org.gbif.pipelines.ingest.java.pipelines.VerbatimToInterpretedPipeline \
 * --runner=SparkRunner \
 * --datasetId=${UUID} \
 * --attempt=1 \
 * --interpretationTypes=ALL \
 * --targetPath=${OUT}\
 * --inputPath=${IN} \
 * --properties=configs/pipelines.yaml \
 * --useExtendedRecordId=true \
 * --useMetadataWsCalls=false \
 * --metaFileName=verbatim-to-occurrence.yml
 *
 * }</pre>
 */
@SuppressWarnings("all")
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToOccurrencePipeline {

  private static final DwcTerm CORE_TERM = DwcTerm.Occurrence;

  public static void main(String[] args) {
    run(args);
  }

  public static void run(String[] args) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options);
  }

  public static void run(InterpretationPipelineOptions options) {
    ExecutorService executor = Executors.newWorkStealingPool();
    try {
      run(options, executor);
    } finally {
      executor.shutdown();
    }
  }

  public static void run(String[] args, ExecutorService executor) {
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);
    run(options, executor);
  }

  public static void run(InterpretationPipelineOptions options, ExecutorService executor) {

    log.info("Pipeline has been started - {}", LocalDateTime.now());
    TransformsFactory transformsFactory = TransformsFactory.create(options);

    String datasetId = options.getDatasetId();
    Integer attempt = options.getAttempt();
    Set<String> types = options.getInterpretationTypes();
    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    // Remove directories with avro files for expected interpretation, except IDENTIFIER
    Set<String> deleteTypes = new HashSet<>(types);
    deleteTypes.remove(IDENTIFIER_ABSENT.name());
    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, deleteTypes);

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    String postfix = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    log.info("Creating pipelines transforms");
    // Core
    MetadataTransform metadataTr = transformsFactory.createMetadataTransform();
    GbifIdTransform gbifIdTr = transformsFactory.createGbifIdTransform();
    GbifIdAbsentTransform gbifIdAbsentTr = transformsFactory.createGbifIdAbsentTransform();
    ClusteringTransform clusteringTr = transformsFactory.createClusteringTransform();
    BasicTransform basicTr = transformsFactory.createBasicTransform();
    TaxonomyTransform taxonomyTr = transformsFactory.createTaxonomyTransform();
    VerbatimTransform verbatimTr = transformsFactory.createVerbatimTransform();
    GrscicollTransform grscicollTr = transformsFactory.createGrscicollTransform();
    LocationTransform locationTr = transformsFactory.createLocationTransform();
    TemporalTransform temporalTr = transformsFactory.createTemporalTransform();
    MultimediaTransform multimediaTr = transformsFactory.createMultimediaTransform();
    AudubonTransform audubonTr = transformsFactory.createAudubonTransform();
    ImageTransform imageTr = transformsFactory.createImageTransform();
    OccurrenceExtensionTransform occExtensionTr =
        transformsFactory.createOccurrenceExtensionTransform();
    ExtensionFilterTransform extensionFilterTr = transformsFactory.createExtensionFilterTransform();
    DefaultValuesTransform defaultValuesTr = transformsFactory.createDefaultValuesTransform();

    try {

      // Create or read MetadataRecord
      MetadataRecord mdr;
      if (useMetadataRecordWriteIO(types)) {
        mdr =
            metadataTr
                .processElement(options.getDatasetId())
                .orElseThrow(() -> new IllegalArgumentException("MetadataRecord can't be null"));

        @Cleanup
        var metadataWriter = createAvroWriter(options, metadataTr, DwcTerm.Occurrence, postfix);
        metadataWriter.append(mdr);
      } else if (useMetadataRecordReadIO(types)) {
        mdr =
            InterpretedAvroReader.readAvroUseTargetPath(options, CORE_TERM, metadataTr)
                .get(options.getDatasetId());
      } else {
        mdr = null;
      }

      // Read DWCA and replace default values
      Map<String, ExtendedRecord> erMap =
          AvroReader.readUniqueRecords(hdfsConfigs, ExtendedRecord.class, options.getInputPath());
      Map<String, ExtendedRecord> erExtMap = occExtensionTr.transform(erMap);
      erExtMap = extensionFilterTr.transform(erExtMap);
      defaultValuesTr.replaceDefaultValues(erExtMap);

      boolean useSyncMode = options.getSyncThreshold() > erExtMap.size();

      // Skip interpretation and use avro reader when partial intepretation is activated
      Function<ExtendedRecord, Optional<IdentifierRecord>> idFn;
      if (useGbifIdWriteIO(types)) {
        log.info("Interpreting GBIF IDs records...");
        idFn = gbifIdTr::processElement;
      } else {
        log.info("Skip GBIF IDs interpretation and reading GBIF IDs from avro files...");
        Map<String, IdentifierRecord> idRecordMap =
            InterpretedAvroReader.readAvroUseTargetPath(options, CORE_TERM, gbifIdTr);
        Map<String, IdentifierRecord> absentIdRecordMap = new HashMap<>();

        if (useAbsentGbifIdReadIO(types)) {
          InterpretedAvroReader.readAvroUseTargetPath(
                  options, gbifIdTr, CORE_TERM, gbifIdTr.getAbsentName())
              .forEach(
                  (k, v) -> {
                    Consumer<IdentifierRecord> fn = ir -> absentIdRecordMap.put(k, ir);
                    gbifIdAbsentTr.processElement(v).ifPresent(fn);
                  });
        }

        idFn =
            er -> {
              IdentifierRecord ir =
                  Optional.ofNullable(idRecordMap.get(er.getId()))
                      .orElse(absentIdRecordMap.get(er.getId()));
              return Optional.ofNullable(ir);
            };
      }

      log.info("Аltering GBIF id duplicates...");
      // Filter GBIF id duplicates
      UniqueGbifIdTransform gbifIdTransform =
          UniqueGbifIdTransform.builder()
              .executor(executor)
              .erMap(erExtMap)
              .idTransformFn(idFn)
              .useSyncMode(useSyncMode)
              .skipTransform(options.isUseExtendedRecordId())
              .counterFn(transformsFactory.getIncMetricFn())
              .build()
              .run();

      log.info("Starting rest of interpretations...");

      if (useGbifIdWriteIO(types) || useAbsentGbifIdReadIO(types)) {
        FsUtils.deleteInterpretIfExist(
            hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, gbifIdTr.getAllNames());
      }

      try (var gbifIdWriter = createAvroWriter(options, gbifIdTr, CORE_TERM, postfix);
          var verbatimWriter = createAvroWriter(options, verbatimTr, CORE_TERM, postfix);
          var clusteringWriter = createAvroWriter(options, clusteringTr, CORE_TERM, postfix);
          var basicWriter = createAvroWriter(options, basicTr, CORE_TERM, postfix);
          var temporalWriter = createAvroWriter(options, temporalTr, CORE_TERM, postfix);
          var multimediaWriter = createAvroWriter(options, multimediaTr, CORE_TERM, postfix);
          var imageWriter = createAvroWriter(options, imageTr, CORE_TERM, postfix);
          var audubonWriter = createAvroWriter(options, audubonTr, CORE_TERM, postfix);
          var taxonWriter = createAvroWriter(options, taxonomyTr, CORE_TERM, postfix);
          var grscicollWriter = createAvroWriter(options, grscicollTr, CORE_TERM, postfix);
          var locationWriter = createAvroWriter(options, locationTr, CORE_TERM, postfix);
          var gbifIdInvalidWriter =
              createAvroWriter(
                  options, gbifIdTr, CORE_TERM, postfix, gbifIdTr.getBaseInvalidName())) {

        // Create interpretation function
        Consumer<ExtendedRecord> interpretAllFn =
            er -> {
              IdentifierRecord idInvalid = gbifIdTransform.getIdInvalidMap().get(er.getId());

              if (idInvalid == null) {
                IdentifierRecord id = gbifIdTransform.getErIdMap().get(er.getId());

                // Can be null if there are GBIF id collisstions and identifiers stage dropped
                // duplicates
                if (id == null) {
                  log.warn(
                      "OccurrenceID {} doesn't have correlated GBIF id (identifiers stage dropped duplicates)",
                      er.getId());
                  return;
                }

                if (clusteringTr.checkType(types)) {
                  clusteringTr.processElement(id).ifPresent(clusteringWriter::append);
                }
                if (verbatimTr.checkType(types)) {
                  verbatimWriter.append(er);
                }
                if (basicTr.checkType(types)) {
                  basicTr.processElement(er).ifPresent(basicWriter::append);
                }
                if (temporalTr.checkType(types)) {
                  temporalTr.processElement(er).ifPresent(temporalWriter::append);
                }
                if (multimediaTr.checkType(types)) {
                  multimediaTr.processElement(er).ifPresent(multimediaWriter::append);
                }
                if (imageTr.checkType(types)) {
                  imageTr.processElement(er).ifPresent(imageWriter::append);
                }
                if (audubonTr.checkType(types)) {
                  audubonTr.processElement(er).ifPresent(audubonWriter::append);
                }
                if (taxonomyTr.checkType(types)) {
                  taxonomyTr.processElement(er).ifPresent(taxonWriter::append);
                }
                if (grscicollTr.checkType(types)) {
                  grscicollTr.processElement(er, mdr).ifPresent(grscicollWriter::append);
                }
                if (locationTr.checkType(types)) {
                  locationTr.processElement(er, mdr).ifPresent(locationWriter::append);
                }
              } else {
                gbifIdInvalidWriter.append(idInvalid);
              }
            };

        // Run async writing for GbifId
        Stream<CompletableFuture<Void>> streamIds = Stream.empty();
        if (useGbifIdWriteIO(types) || useAbsentGbifIdReadIO(types)) {
          Collection<IdentifierRecord> idCollection = gbifIdTransform.getIdMap().values();
          if (useSyncMode) {
            streamIds =
                Stream.of(
                    CompletableFuture.runAsync(
                        () -> idCollection.forEach(gbifIdWriter::append), executor));
          } else {
            streamIds =
                idCollection.stream()
                    .map(v -> CompletableFuture.runAsync(() -> gbifIdWriter.append(v), executor));
          }
        }

        // Run async interpretation and writing for all records
        Stream<CompletableFuture<Void>> streamAll;
        Collection<ExtendedRecord> erCollection = erExtMap.values();
        if (useSyncMode) {
          streamAll =
              Stream.of(
                  CompletableFuture.runAsync(() -> erCollection.forEach(interpretAllFn), executor));
        } else {
          streamAll =
              erCollection.stream()
                  .map(v -> CompletableFuture.runAsync(() -> interpretAllFn.accept(v), executor));
        }

        // Wait for all features
        CompletableFuture<?>[] futures =
            Stream.concat(streamIds, streamAll).toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).get();
      }

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage());
      throw new IllegalStateException("Failed performing conversion on ", e);
    } finally {
      Shutdown.doOnExit(basicTr, locationTr, taxonomyTr, grscicollTr, gbifIdTr);
    }

    log.info("Save metrics into the file and set files owner");
    String metadataPath =
        PathBuilder.buildDatasetAttemptPath(options, options.getMetaFileName(), false);
    if (!FsUtils.fileExists(hdfsConfigs, metadataPath) || useGbifIdWriteIO(types)) {
      MetricsHandler.saveCountersToTargetPathFile(
          options, transformsFactory.getMetrics().getMetricsResult());
    }

    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }

  private static boolean useGbifIdWriteIO(Set<String> types) {
    return types.contains(RecordType.IDENTIFIER.name()) || types.contains(RecordType.ALL.name());
  }

  private static boolean useAbsentGbifIdReadIO(Set<String> types) {
    return types.contains(RecordType.IDENTIFIER_ABSENT.name());
  }

  private static boolean useMetadataRecordWriteIO(Set<String> types) {
    return types.contains(RecordType.METADATA.name()) || types.contains(RecordType.ALL.name());
  }

  private static boolean useMetadataRecordReadIO(Set<String> types) {
    return types.contains(RecordType.LOCATION.name())
        || types.contains(RecordType.GRSCICOLL.name());
  }
}
