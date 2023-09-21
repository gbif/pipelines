package org.gbif.pipelines.ingest.java.pipelines;

import static org.gbif.pipelines.common.PipelinesVariables.Metrics.DUPLICATE_IDS_COUNT;
import static org.gbif.pipelines.ingest.java.transforms.InterpretedAvroWriter.createAvroWriter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.metrics.MetricsHandler;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.AvroReader;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.ingest.java.pipelines.interpretation.TransformsFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transforms.common.ExtensionFilterTransform;
import org.gbif.pipelines.transforms.java.OccurrenceExtensionTransform;
import org.gbif.pipelines.transforms.java.UniqueGbifIdTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.slf4j.MDC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class VerbatimToIdentifierPipeline {
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
    String targetPath = options.getTargetPath();
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(options.getHdfsSiteConfig(), options.getCoreSiteConfig());

    MDC.put("datasetKey", datasetId);
    MDC.put("attempt", attempt.toString());
    MDC.put("step", StepType.VERBATIM_TO_INTERPRETED.name());

    String postfix = Long.toString(LocalDateTime.now().toEpochSecond(ZoneOffset.UTC));

    log.info("Creating pipelines transforms");
    // Core
    GbifIdTransform gbifIdTr = transformsFactory.createGbifIdTransform();
    OccurrenceExtensionTransform occExtensionTr =
        transformsFactory.createOccurrenceExtensionTransform();
    ExtensionFilterTransform extensionFilterTr = transformsFactory.createExtensionFilterTransform();

    FsUtils.deleteInterpretIfExist(
        hdfsConfigs, targetPath, datasetId, attempt, CORE_TERM, gbifIdTr.getAllNames());

    try {

      // Read DWCA and replace default values
      Map<String, ExtendedRecord> erMap =
          AvroReader.readUniqueRecords(
              hdfsConfigs,
              ExtendedRecord.class,
              options.getInputPath(),
              () -> transformsFactory.getMetrics().incMetric(DUPLICATE_IDS_COUNT));

      Map<String, ExtendedRecord> erExtMap = occExtensionTr.transform(erMap);
      erExtMap = extensionFilterTr.transform(erExtMap);

      boolean useSyncMode = options.getSyncThreshold() > erExtMap.size();

      UniqueGbifIdTransform gbifIdTransform =
          UniqueGbifIdTransform.builder()
              .executor(executor)
              .erMap(erExtMap)
              .idTransformFn(gbifIdTr::processElement)
              .useSyncMode(useSyncMode)
              .skipTransform(options.isUseExtendedRecordId())
              .counterFn(transformsFactory.getIncMetricFn())
              .build()
              .run();

      try (var gbifIdWriter = createAvroWriter(options, gbifIdTr, CORE_TERM, postfix);
          var gbifIdAbsentWriter =
              createAvroWriter(options, gbifIdTr, CORE_TERM, postfix, gbifIdTr.getAbsentName());
          var gbifIdInvalidWriter =
              createAvroWriter(
                  options, gbifIdTr, CORE_TERM, postfix, gbifIdTr.getBaseInvalidName())) {

        CompletableFuture<Void> idFtr =
            CompletableFuture.runAsync(
                () -> gbifIdTransform.getIdMap().values().forEach(gbifIdWriter::append));

        CompletableFuture<Void> idAbsentFtr =
            CompletableFuture.runAsync(
                () ->
                    gbifIdTransform.getIdAbsentMap().values().forEach(gbifIdAbsentWriter::append));

        CompletableFuture<Void> idInvalidFtr =
            CompletableFuture.runAsync(
                () ->
                    gbifIdTransform
                        .getIdInvalidMap()
                        .values()
                        .forEach(gbifIdInvalidWriter::append));

        // Wait for all features
        CompletableFuture.allOf(idFtr, idAbsentFtr, idInvalidFtr).get();
      }

    } catch (Exception e) {
      log.error("Failed performing conversion on {}", e.getMessage());
      throw new IllegalStateException("Failed performing conversion on ", e);
    }

    MetricsHandler.saveCountersToTargetPathFile(
        options, transformsFactory.getMetrics().getMetricsResult());
    log.info("Pipeline has been finished - {}", LocalDateTime.now());
  }
}
