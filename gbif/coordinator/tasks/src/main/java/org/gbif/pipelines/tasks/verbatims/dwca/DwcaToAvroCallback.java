package org.gbif.pipelines.tasks.verbatims.dwca;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.getAllInterpretationAsString;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.getAllValidatorInterpretationAsString;
import static org.gbif.pipelines.common.ValidatorPredicate.isValidator;
import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.CodecFactory;
import org.gbif.api.model.crawler.GenericValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.PipelinesWorkflow.Graph;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.converters.DwcaToAvroConverter;
import org.gbif.dwc.Archive;
import org.gbif.dwc.UnsupportedArchiveException;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.DwcaUtils;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesDwcaMessage} is received. */
@Slf4j
@Builder
public class DwcaToAvroCallback extends AbstractMessageCallback<PipelinesDwcaMessage>
    implements StepHandler<PipelinesDwcaMessage, PipelinesVerbatimMessage> {

  private final DwcaToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;
  private final ValidationWsClient validationClient;

  @Override
  public void handleMessage(PipelinesDwcaMessage message) {

    boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);
    StepType type = isValidator ? StepType.VALIDATOR_DWCA_TO_VERBATIM : StepType.DWCA_TO_VERBATIM;

    PipelinesCallback.<PipelinesDwcaMessage, PipelinesVerbatimMessage>builder()
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .validationClient(validationClient)
        .config(config)
        .stepType(type)
        .isValidator(isValidator)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    PipelinesDwcaMessage message = new PipelinesDwcaMessage();
    if (config.validatorOnly) {
      message.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_DWCA_TO_VERBATIM.name()));
    }
    return message.getRoutingKey();
  }

  /** Only correct messages can be handled, by now is only OCCURRENCE type messages */
  @Override
  public boolean isMessageCorrect(PipelinesDwcaMessage message) {
    boolean isPlatformCorrect = Platform.PIPELINES.equivalent(message.getPlatform());
    if (!isPlatformCorrect) {
      log.info("Skipping the task, because of the platform {} is incorrect", message.getPlatform());
    }
    boolean isMessageValid =
        message.getDatasetType() != null && message.getValidationReport().isValid();
    if (!isMessageValid) {
      log.info(
          "Skipping the task, because of the message is not valid, data type is {}, validation report is valid = {}",
          message.getDatasetType(),
          message.getValidationReport().isValid());
    }
    boolean isReportValid = isReportValid(message);
    if (!isReportValid) {
      log.info(
          "Skipping the task, because of the validation report is missed or there are no records");
    }
    return isPlatformCorrect && isMessageValid && isReportValid;
  }

  private boolean isReportValid(PipelinesDwcaMessage message) {
    boolean isValidOccurrenceReport =
        message.getValidationReport().getOccurrenceReport() != null
            && message.getValidationReport().getOccurrenceReport().getCheckedRecords() > 0;
    boolean isValidGenericReport =
        message.getValidationReport().getGenericReport() != null
            && message.getValidationReport().getGenericReport().getCheckedRecords() > 0;
    return isValidOccurrenceReport || isValidGenericReport;
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

      HdfsConfigs hdfsConfigs =
          HdfsConfigs.create(config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig);
      // Run main conversion process
      DwcaToAvroConverter.create()
          .codecFactory(CodecFactory.fromString(config.avroConfig.compressionType))
          .syncInterval(config.avroConfig.syncInterval)
          .hdfsConfigs(hdfsConfigs)
          .inputPath(inputPath)
          .outputPath(outputPath)
          .metaPath(metaPath)
          .skipDeletion(isValidator(message.getPipelineSteps(), config.validatorOnly))
          .convert();
    };
  }

  @SneakyThrows
  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesDwcaMessage message) {
    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    Set<String> interpretedTypes = config.interpretTypes;

    try {
      boolean isValidator = isValidator(message.getPipelineSteps(), config.validatorOnly);

      Path inputPath = buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid());
      Archive archive =
          isValidator
              ? DwcaUtils.fromLocationSkipValidation(inputPath)
              : DwcaUtils.fromLocation(inputPath);

      if (message.getPipelineSteps().isEmpty()) {

        Graph<StepType> workflow;
        if (isValidator) {
          workflow = PipelinesWorkflow.getValidatorWorkflow();
        } else {
          Term coreType = archive.getCore().getRowType();
          boolean hasOccExt =
              archive.getExtensions().stream().anyMatch(x -> x.getRowType() == DwcTerm.Occurrence);

          boolean hasOccurrences = coreType == DwcTerm.Occurrence || hasOccExt;
          boolean hasEvents = coreType == DwcTerm.Event;

          workflow = PipelinesWorkflow.getWorkflow(hasOccurrences, hasEvents);
        }

        StepType type =
            isValidator ? StepType.VALIDATOR_DWCA_TO_VERBATIM : StepType.DWCA_TO_VERBATIM;

        Set<String> steps =
            workflow.getAllNodesFor(Collections.singleton(type)).stream()
                .map(StepType::name)
                .collect(Collectors.toSet());

        message.setPipelineSteps(steps);
      }

      // Calculates and checks existence of DwC Archive
      Set<String> allInterpretationAsString =
          isValidator ? getAllValidatorInterpretationAsString() : getAllInterpretationAsString();

      interpretedTypes = DwcaUtils.getExtensionAsTerms(archive);
      interpretedTypes.addAll(allInterpretationAsString);
      interpretedTypes.remove(null);
    } catch (IllegalStateException | UnsupportedArchiveException ex) {
      Set<String> steps =
          PipelinesWorkflow.getOccurrenceWorkflow().getAllNodes().stream()
              .map(StepType::name)
              .collect(Collectors.toSet());
      message.setPipelineSteps(steps);
      log.warn(ex.getMessage(), ex);
    }

    // Common variables
    OccurrenceValidationReport report = message.getValidationReport().getOccurrenceReport();
    Long numberOfRecords = report == null ? null : (long) report.getCheckedRecords();
    GenericValidationReport genericReport = message.getValidationReport().getGenericReport();
    Long numberOfEventRecords =
        genericReport == null ? null : (long) genericReport.getCheckedRecords();
    ValidationResult validationResult =
        new ValidationResult(
            tripletsValid(report),
            occurrenceIdsValid(report),
            null,
            numberOfRecords,
            numberOfEventRecords);

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
        message.getDatasetType());
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
