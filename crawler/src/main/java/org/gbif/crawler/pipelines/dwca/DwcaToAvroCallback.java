package org.gbif.crawler.pipelines.dwca;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.converters.DwcaToAvroConverter;
import org.gbif.crawler.common.utils.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.avro.file.CodecFactory;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import static org.gbif.crawler.common.utils.HdfsUtils.buildOutputPath;
import static org.gbif.crawler.common.utils.HdfsUtils.buildOutputPathAsString;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Callback which is called when the {@link PipelinesDwcaMessage} is received.
 * <p>
 * The main method is {@link DwcaToAvroCallback#handleMessage}
 */
public class DwcaToAvroCallback extends AbstractMessageCallback<PipelinesDwcaMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToAvroCallback.class);
  private static final StepType STEP = StepType.DWCA_TO_VERBATIM;

  private final DwcaToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient historyWsClient;

  DwcaToAvroCallback(DwcaToAvroConfiguration config, MessagePublisher publisher, CuratorFramework curator,
                     PipelinesHistoryWsClient historyWsClient) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.publisher = publisher;
    this.historyWsClient = historyWsClient;
  }

  /**
   * Handles a MQ {@link PipelinesDwcaMessage} message
   */
  @Override
  public void handleMessage(PipelinesDwcaMessage message) {

    if (!Platform.PIPELINES.equivalent(message.getPlatform())) {
      LOG.info("Skip message because pipelines don't support the platform {}", message);
      return;
    }

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    try (MDCCloseable mdc1 = MDC.putCloseable("datasetId", datasetId.toString());
        MDCCloseable mdc2 = MDC.putCloseable("attempt", attempt.toString());
        MDCCloseable mdc3 = MDC.putCloseable("step", STEP.name())) {

      LOG.info("Message handler began - {}", message);

      if (!isMessageCorrect(message)) {
        LOG.info("Skip the message, cause it is not correct or it wasn't modified, exit from handler");
        return;
      }

      if (message.getPipelineSteps().isEmpty()) {
        message.setPipelineSteps(Sets.newHashSet(
            StepType.DWCA_TO_VERBATIM.name(),
            StepType.VERBATIM_TO_INTERPRETED.name(),
            StepType.INTERPRETED_TO_INDEX.name(),
            StepType.HDFS_VIEW.name()
        ));
      }

      // Common variables
      Set<String> steps = message.getPipelineSteps();
      Runnable runnable = createRunnable(message);
      EndpointType endpointType = message.getEndpointType();
      OccurrenceValidationReport occReport = message.getValidationReport().getOccurrenceReport();
      Long numberOfRecords = occReport == null ? null : (long) occReport.getCheckedRecords();
      ValidationResult validationResult =
          new ValidationResult(tripletsValid(occReport), occurrenceIdsValid(occReport), null, numberOfRecords);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.create()
          .incomingMessage(message)
          .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps, endpointType, validationResult))
          .curator(curator)
          .zkRootElementPath(STEP.getLabel())
          .pipelinesStepName(STEP)
          .publisher(publisher)
          .runnable(runnable)
          .historyWsClient(historyWsClient)
          .metricsSupplier(metricsSupplier(datasetId, attempt))
          .build()
          .handleMessage();

      LOG.info("Message handler ended - {}", message);

    }
  }

  /**
   * Only correct messages can be handled, by now is only OCCURRENCE type messages
   */
  private boolean isMessageCorrect(PipelinesDwcaMessage message) {
    return message.getDatasetType() != null && message.getValidationReport().isValid()
        && message.getValidationReport().getOccurrenceReport().getCheckedRecords() > 0;
  }

  /**
   * Main message processing logic, converts a DwCA archive to an avro file.
   */
  private Runnable createRunnable(PipelinesDwcaMessage message) {
    return () -> {

      Optional.ofNullable(message.getEndpointType())
          .orElseThrow(() -> new IllegalArgumentException("endpointType can't be NULL!"));

      UUID datasetId = message.getDatasetUuid();
      String attempt = String.valueOf(message.getAttempt());

      // Calculates and checks existence of DwC Archive
      Path inputPath = buildInputPath(config.archiveRepository, datasetId);

      // Calculates export path of avro as extended record
      org.apache.hadoop.fs.Path outputPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.fileName);

      // Calculates metadata path, the yaml file with total number of converted records
      org.apache.hadoop.fs.Path metaPath =
          buildOutputPath(config.repositoryPath, datasetId.toString(), attempt, config.metaFileName);

      // Run main conversion process
      DwcaToAvroConverter.create()
          .codecFactory(CodecFactory.fromString(config.avroConfig.compressionType))
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsSiteConfig(config.hdfsSiteConfig)
          .inputPath(inputPath)
          .outputPath(outputPath)
          .metaPath(metaPath)
          .convert();
    };
  }

  /**
   * Input path example - /mnt/auto/crawler/dwca/9bed66b3-4caa-42bb-9c93-71d7ba109dad
   */
  private Path buildInputPath(String archiveRepository, UUID dataSetUuid) {
    Path directoryPath = Paths.get(archiveRepository, dataSetUuid.toString());
    Preconditions.checkState(directoryPath.toFile().exists(), "Directory - %s does not exist!", directoryPath);

    return directoryPath;
  }

  /**
   * For XML datasets triplets are always valid. For DwC-A datasets triplets are valid if there are more than 0 unique
   * triplets in the dataset, and exactly 0 triplets referenced by more than one record.
   */
  private static boolean tripletsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getUniqueTriplets() > 0
        && report.getCheckedRecords() - report.getRecordsWithInvalidTriplets() == report.getUniqueTriplets();
  }

  /**
   * For XML datasets occurrenceIds are always accepted. For DwC-A datasets occurrenceIds are valid if each record has a
   * unique occurrenceId.
   */
  private static boolean occurrenceIdsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getCheckedRecords() > 0 && report.getUniqueOccurrenceIds() == report.getCheckedRecords();
  }

  private Supplier<List<PipelineStep.MetricInfo>> metricsSupplier(UUID datasetId, int attempt) {
    return () ->
        HdfsUtils.readMetricsFromMetaFile(
            config.hdfsSiteConfig,
            buildOutputPathAsString(
                config.repositoryPath,
                datasetId.toString(),
                String.valueOf(attempt),
                config.metaFileName));
  }
}
