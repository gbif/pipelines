package org.gbif.pipelines.tasks.validators.checklist;

import static org.gbif.validator.api.Metrics.ValidationStep;
import static org.gbif.validator.api.Validation.Status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.OutputKeys;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.pipelines.orchestrator.OrchestratorReplies;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.checklist.ChecklistValidator;
import org.gbif.pipelines.validator.serde.ObjectMapperUtils;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesChecklistValidatorMessage} is received. */
@Slf4j
public class ChecklistValidatorCallback
    extends AbstractMessageCallback<PipelinesChecklistValidatorMessage> {

  private static final String STEP_TYPE = StepType.VALIDATOR_VALIDATE_ARCHIVE.name();

  private static final ObjectMapper OBJECT_MAPPER =
      ObjectMapperUtils.createObjectMapperWithColDPSupport();

  private final ChecklistValidatorConfiguration config;
  private final ChecklistValidator checklistValidator;
  private final ValidationWsClient validationClient;
  private final MessagePublisher messagePublisher;

  @SneakyThrows
  public ChecklistValidatorCallback(
      ChecklistValidatorConfiguration config,
      ValidationWsClient validationClient,
      MessagePublisher messagePublisher) {
    this.config = config;
    this.validationClient = validationClient;
    this.checklistValidator =
        new ChecklistValidator(
            config.registry.wsUrl,
            config.clbConfig.url,
            config.clbConfig.user,
            config.clbConfig.password,
            null);
    this.messagePublisher = messagePublisher;
  }

  // useful to mock the ChecklistbankWsClient in the ChecklistValidator for tests
  public ChecklistValidatorCallback(
      ChecklistValidatorConfiguration config,
      ValidationWsClient validationClient,
      MessagePublisher messagePublisher,
      ChecklistValidator checklistValidator) {
    this.config = config;
    this.validationClient = validationClient;
    this.checklistValidator = checklistValidator;
    this.messagePublisher = messagePublisher;
  }

  @Override
  public void handleMessage(PipelinesChecklistValidatorMessage message) {
    Optional<StepHeader> header = OrchestratorReplies.header(message.getStepHeader());
    if (header.isPresent()) {
      handleForOrchestrator(message, header.get());
      return;
    }
    Validation validation = validationClient.get(message.getValidationKey());

    if (validation == null) {
      log.error("Validation not found for key {}", message.getValidationKey());
      return;
    }

    if (!validation.isExecuting()) {
      updateStatus(validation, Status.FAILED);
      return;
    }

    updateStatus(validation, Status.RUNNING);

    try {
      ClbDatasetImport clbDatasetImport =
          OBJECT_MAPPER.readValue(message.getClBResponsePayload(), ClbDatasetImport.class);

      if (validation.getClbDatasetKey() == null
          || validation.getClbDatasetKey() != clbDatasetImport.getDatasetKey()) {
        log.info(
            "CLB Dataset key {} is different from validation clb dataset key {}",
            clbDatasetImport.getDatasetKey(),
            validation.getClbDatasetKey());
        updateStatus(validation, Status.FAILED);
        return;
      }

      if (clbDatasetImport.getStatus().equalsIgnoreCase(ClbDatasetImport.FAILED)) {
        updateStatus(validation, Status.FAILED);
      } else if (clbDatasetImport.getStatus().equalsIgnoreCase(ClbDatasetImport.CANCELED)) {
        updateStatus(validation, Status.ABORTED);
      } else if (clbDatasetImport.getStatus().equalsIgnoreCase(ClbDatasetImport.FINISHED)) {
        try {
          List<Metrics.FileInfo> result = checklistValidator.evaluateResults(clbDatasetImport);
          log.info(
              "Validating DWCA checklist archive - finished calling checklistbank, merging results");
          result.forEach(fileInfo -> Validations.mergeFileInfo(validation, fileInfo));

          if (validation.getFileFormat() == FileFormat.COLDP) {
            validation.getMetrics().setIndexeable(true);
          }

          updateStatus(validation, Status.FINISHED);

          // send message to continue the process
          messagePublisher.send(
              checklistValidator.createNextMessage(validation.getClbValidationMessage()));
          log.info(
              "Next message for checklist validation {} has been sent", message.getValidationKey());

        } catch (Exception e) {
          log.error(
              "Error processing CLB validation results for {}", message.getValidationKey(), e);
          updateStatus(validation, Status.FAILED);
        }
      } else {
        log.info(
            "Setting validation {} to FAILED since there is no valid clb validation",
            message.getValidationKey());
        updateStatus(validation, Status.FAILED);
      }
    } catch (JsonProcessingException e) {
      log.error(
          "Setting validation {} to FAILED because of a processing error",
          message.getValidationKey(),
          e);
      updateStatus(validation, Status.FAILED);
    }
  }

  /**
   * Orchestrator mode: the validation web service hands over the ChecklistBank import it was
   * waiting for, with the VALIDATOR_CHECKLISTBANK step's header. A finished import is evaluated and
   * reported as {@code CHECKLIST}; any other state (failed, canceled, still running after the wait,
   * or no import) fails the step. No validator-ws calls and no next message.
   */
  void handleForOrchestrator(PipelinesChecklistValidatorMessage message, StepHeader header) {
    Instant started = Instant.now();
    StepCompleted completed;
    try {
      completed =
          OrchestratorReplies.succeeded(
              header,
              started,
              Map.of(OutputKeys.DATASET_TYPE, DatasetType.CHECKLIST.name()),
              List.of(OrchestratorReplies.report(ReportTypes.CHECKLIST, evaluate(message))));
    } catch (Exception e) {
      log.error("ChecklistBank evaluation of {} failed", message.getValidationKey(), e);
      completed = OrchestratorReplies.failed(header, started, e);
    }
    try {
      new OrchestratorReplies(messagePublisher, config.orchestrator.toConfig()).send(completed);
    } catch (Exception e) {
      // The step stays dispatched until the orchestrator's timeout handling.
      log.error("Could not reply to the orchestrator for {}", header, e);
    }
  }

  /**
   * The {@code CHECKLIST} report: {@code {"clbDatasetKey", "importStatus", "indexeable", "files"}};
   * a ColDP is indexeable once evaluated, as validator-ws marked it.
   */
  private Map<String, Object> evaluate(PipelinesChecklistValidatorMessage message)
      throws Exception {
    if (message.getClBResponsePayload() == null) {
      throw new IllegalStateException("ChecklistBank sent no import");
    }
    ClbDatasetImport clbImport =
        OBJECT_MAPPER.readValue(message.getClBResponsePayload(), ClbDatasetImport.class);
    if (!ClbDatasetImport.FINISHED.equalsIgnoreCase(clbImport.getStatus())) {
      throw new IllegalStateException(
          "ChecklistBank import of dataset "
              + clbImport.getDatasetKey()
              + " is "
              + clbImport.getStatus()
              + ", not finished");
    }
    List<Metrics.FileInfo> files = checklistValidator.evaluateResults(clbImport);
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("clbDatasetKey", clbImport.getDatasetKey());
    report.put("importStatus", clbImport.getStatus());
    report.put("indexeable", "coldp".equalsIgnoreCase(clbImport.getFormat()) ? Boolean.TRUE : null);
    report.put("files", files);
    return report;
  }

  public Validation updateStatus(Validation validation, Status newStatus) {

    // In case the validation has already finished we don't need to update the status
    if (validation.hasFinished()) {
      return validation;
    }

    validation.setModified(Timestamp.valueOf(ZonedDateTime.now().toLocalDateTime()));

    Metrics metrics =
        Optional.ofNullable(validation.getMetrics()).orElse(Metrics.builder().build());

    for (ValidationStep step : metrics.getStepTypes()) {
      if (step.getStepType().equals(STEP_TYPE)) {
        step.setStatus(newStatus);
        break;
      }
    }

    Status mainStatus = newStatus;
    if (mainStatus == Status.FINISHED) {
      boolean isQueued =
          validation.getMetrics().getStepTypes().stream()
              .filter(x -> !x.getStepType().equals(STEP_TYPE))
              .anyMatch(x -> x.getStatus() != Status.FINISHED);
      if (isQueued) {
        mainStatus = Status.QUEUED;
      }
    }
    validation.setStatus(mainStatus);

    validation.setMetrics(metrics);

    log.info("Validation {} change status to {} for {}", validation.getKey(), newStatus, STEP_TYPE);
    return validationClient.update(validation.getKey(), validation);
  }
}
