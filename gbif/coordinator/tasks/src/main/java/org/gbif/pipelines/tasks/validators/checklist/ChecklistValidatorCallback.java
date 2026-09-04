package org.gbif.pipelines.tasks.validators.checklist;

import static org.gbif.validator.api.Metrics.ValidationStep;
import static org.gbif.validator.api.Validation.Status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.pipelines.validator.Validations;
import org.gbif.pipelines.validator.checklist.ChecklistValidator;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesChecklistValidatorMessage} is received. */
@Slf4j
public class ChecklistValidatorCallback
    extends AbstractMessageCallback<PipelinesChecklistValidatorMessage> {

  private static final String STEP_TYPE = StepType.VALIDATOR_VALIDATE_ARCHIVE.name();

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
            config.clbConfig.url, config.clbConfig.user, config.clbConfig.password, null);
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

  public Validation updateStatus(Validation validation, Status newStatus) {

    // In case when validation was finihsed we need don't need to update the status
    if (validation.hasFinished()) {
      return validation;
    }

    validation.setStatus(newStatus);
    validation.setModified(Timestamp.valueOf(ZonedDateTime.now().toLocalDateTime()));

    Metrics metrics =
        Optional.ofNullable(validation.getMetrics()).orElse(Metrics.builder().build());

    for (ValidationStep step : metrics.getStepTypes()) {
      if (step.getStepType().equals(STEP_TYPE)) {
        step.setStatus(newStatus);
        break;
      }
    }

    validation.setMetrics(metrics);

    log.info("Validation {} change status to {} for {}", validation.getKey(), newStatus, STEP_TYPE);
    return validationClient.update(validation.getKey(), validation);
  }
}
