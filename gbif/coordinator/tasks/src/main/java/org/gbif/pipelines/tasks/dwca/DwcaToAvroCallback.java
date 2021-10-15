package org.gbif.pipelines.tasks.dwca;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.getAllInterpretationAsString;
import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.converters.DwcaToAvroConverter;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesDwcaMessage} is received. */
@Slf4j
public class DwcaToAvroCallback extends AbstractMessageCallback<PipelinesDwcaMessage>
    implements StepHandler<PipelinesDwcaMessage, PipelinesVerbatimMessage> {

  private final DwcaToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;

  public DwcaToAvroCallback(
      DwcaToAvroConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ValidationWsClient validationClient) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.historyClient = historyClient;
    this.validationClient = validationClient;
  }

  @Override
  public void handleMessage(PipelinesDwcaMessage message) {
    StepType type =
        message.isValidator() || config.validatorOnly
            ? StepType.VALIDATOR_DWCA_TO_VERBATIM
            : StepType.DWCA_TO_VERBATIM;
    PipelinesCallback.<PipelinesDwcaMessage, PipelinesVerbatimMessage>builder()
        .historyClient(historyClient)
        .validationClient(validationClient)
        .config(config)
        .curator(curator)
        .stepType(type)
        .isValidator(message.isValidator())
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  /** Only correct messages can be handled, by now is only OCCURRENCE type messages */
  @Override
  public boolean isMessageCorrect(PipelinesDwcaMessage message) {
    boolean isPlatformCorrect = Platform.PIPELINES.equivalent(message.getPlatform());
    boolean isMessageValid =
        message.getDatasetType() != null && message.getValidationReport().isValid();
    boolean isReportValid =
        message.getValidationReport().getOccurrenceReport() != null
            && message.getValidationReport().getOccurrenceReport().getCheckedRecords() > 0;
    return isPlatformCorrect && isMessageValid && isReportValid;
  }

  /** Main message processing logic, converts a DwCA archive to an avro file. */
  @Override
  public Runnable createRunnable(PipelinesDwcaMessage message) {
    return () -> {
      UUID datasetId = message.getDatasetUuid();
      String attempt = String.valueOf(message.getAttempt());

      // Calculates and checks existence of DwC Archive
      Path inputPath = buildDwcaInputPath(config.archiveRepository, datasetId);

      // Calculates export path of avro as extended record
      org.apache.hadoop.fs.Path outputPath =
          HdfsUtils.buildOutputPath(
              config.stepConfig.repositoryPath, datasetId.toString(), attempt, config.fileName);

      // Calculates metadata path, the yaml file with total number of converted records
      org.apache.hadoop.fs.Path metaPath =
          HdfsUtils.buildOutputPath(
              config.stepConfig.repositoryPath, datasetId.toString(), attempt, config.metaFileName);

      // Run main conversion process
      DwcaToAvroConverter.create()
          .codecFactory(CodecFactory.fromString(config.avroConfig.compressionType))
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsSiteConfig(config.stepConfig.hdfsSiteConfig)
          .coreSiteConfig(config.stepConfig.coreSiteConfig)
          .inputPath(inputPath)
          .outputPath(outputPath)
          .metaPath(metaPath)
          .skipDeletion(message.isValidator() || config.validatorOnly)
          .convert();
    };
  }

  @SneakyThrows
  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesDwcaMessage message) {
    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(
          new HashSet<>(
              Arrays.asList(
                  StepType.DWCA_TO_VERBATIM.name(),
                  StepType.VERBATIM_TO_INTERPRETED.name(),
                  StepType.INTERPRETED_TO_INDEX.name(),
                  StepType.HDFS_VIEW.name(),
                  StepType.FRAGMENTER.name())));
    }

    // Calculates and checks existence of DwC Archive
    Set<String> interpretedTypes = config.interpretTypes;
    try {
      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      interpretedTypes = DwcaUtils.getExtensionAsTerms(DwcaUtils.fromLocation(inputPath));
      interpretedTypes.addAll(getAllInterpretationAsString());
      interpretedTypes.remove(null);
    } catch (IllegalStateException ex) {
      log.warn(ex.getMessage());
    }

    // Common variables
    OccurrenceValidationReport report = message.getValidationReport().getOccurrenceReport();
    Long numberOfRecords = report == null ? null : (long) report.getCheckedRecords();
    ValidationResult validationResult =
        new ValidationResult(
            tripletsValid(report), occurrenceIdsValid(report), null, numberOfRecords);

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        interpretedTypes,
        message.getPipelineSteps(),
        null,
        message.getEndpointType(),
        null,
        validationResult,
        null,
        null,
        message.isValidator() || config.validatorOnly);
  }

  /**
   * For XML datasets triplets are always valid. For DwC-A datasets triplets are valid if there are
   * more than 0 unique triplets in the dataset, and exactly 0 triplets referenced by more than one
   * record.
   */
  private boolean tripletsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getUniqueTriplets() > 0
        && report.getCheckedRecords() - report.getRecordsWithInvalidTriplets()
            == report.getUniqueTriplets();
  }

  /**
   * For XML datasets occurrenceIds are always accepted. For DwC-A datasets occurrenceIds are valid
   * if each record has a unique occurrenceId.
   */
  private boolean occurrenceIdsValid(OccurrenceValidationReport report) {
    if (report == null) {
      return true;
    }
    return report.getCheckedRecords() > 0
        && report.getUniqueOccurrenceIds() == report.getCheckedRecords();
  }
}
