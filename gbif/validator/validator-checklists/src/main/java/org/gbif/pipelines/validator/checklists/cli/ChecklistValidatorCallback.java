package org.gbif.pipelines.validator.checklists.cli;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.checklistbank.cli.common.NeoConfiguration;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.pipelines.validator.checklists.ChecklistValidator;
import org.gbif.pipelines.validator.checklists.cli.config.ChecklistValidatorConfiguration;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesChecklistValidatorMessage} is received. */
@Slf4j
public class ChecklistValidatorCallback
    extends AbstractMessageCallback<PipelinesChecklistValidatorMessage> {

  // Use stepType as a String to keep validation api separate to gbif-api
  private static final String STEP_TYPE = "VALIDATOR_COLLECT_METRICS";

  private final ChecklistValidatorConfiguration config;
  private final ChecklistValidator checklistValidator;
  private final ValidationWsClient validationClient;
  private final MessagePublisher messagePublisher;

  public ChecklistValidatorCallback(
      ChecklistValidatorConfiguration config,
      ValidationWsClient validationClient,
      MessagePublisher messagePublisher) {
    this.config = config;
    this.validationClient = validationClient;
    this.checklistValidator = new ChecklistValidator(toNeoConfiguration(config));
    this.messagePublisher = messagePublisher;
  }

  /** Creates a NeoConfiguration from the pipeline configuration. */
  private static NeoConfiguration toNeoConfiguration(ChecklistValidatorConfiguration config) {
    NeoConfiguration neoConfiguration = new NeoConfiguration();
    neoConfiguration.mappedMemory = config.neoMappedMemory;
    neoConfiguration.neoRepository = config.neoRepository;
    neoConfiguration.port = config.neoPort;
    neoConfiguration.batchSize = config.neoBatchSize;
    return neoConfiguration;
  }

  /** Input path example - /mnt/auto/crawler/dwca/9bed66b3-4caa-42bb-9c93-71d7ba109dad */
  public static Path buildDwcaInputPath(String archiveRepository, UUID dataSetUuid) {
    Path directoryPath = Paths.get(archiveRepository, dataSetUuid.toString());
    if (!directoryPath.toFile().exists()) {
      throw new IllegalStateException("Directory does not exist! - " + directoryPath);
    }
    return directoryPath;
  }

  @Override
  @SneakyThrows
  public void handleMessage(PipelinesChecklistValidatorMessage message) {
    Validation validation = validationClient.get(message.getDatasetUuid());

    if (validation != null) {
      updateStatusToRunning(validation);
      if (!validation.hasFinished()) {
        validateArchive(validation);
      }
      // void send(Object message, String exchange, String routingKey, boolean persistent, String
      // correlationId, String replyTo)
      messagePublisher.replyToQueue(
          Boolean.TRUE, true, message.getCorrelationId(), message.getReplyTo());
    } else {
      log.error("Checklist validation started: {}", message);
    }
  }

  /** Performs the validation and update of validation data. */
  private void validateArchive(Validation validation) {
    try {
      log.info("Validating checklist archive: {}", validation.getKey());
      List<Metrics.FileInfo> report =
          checklistValidator.evaluate(
              buildDwcaInputPath(config.archiveRepository, validation.getKey()));
      updateValidationFinished(validation, report);
    } catch (Exception ex) {
      log.error("Error validating checklist", ex);
      updateFailedValidation(validation);
    }
  }

  /** Updates the data of a failed validation */
  private void updateFailedValidation(Validation validation) {
    validation.setStatus(Validation.Status.FAILED);
    validationClient.update(validation);
  }

  /** Updates the data of a successful validation */
  private void updateValidationFinished(Validation validation, List<Metrics.FileInfo> report) {
    validation
        .getMetrics()
        .setFileInfos(mergeFileInfoLists(validation.getMetrics().getFileInfos(), report));
    validationClient.update(validation);
    log.info("Checklist validation finished: {}", validation.getKey());
  }

  private static List<Metrics.FileInfo> mergeFileInfoLists(
      List<Metrics.FileInfo> from, List<Metrics.FileInfo> to) {
    List<Metrics.FileInfo> result = new ArrayList<>(to);
    if (from != null) {
      result.addAll(
          from.stream()
              .filter(
                  fi -> to.stream().noneMatch(nfi -> nfi.getFileName().equals(fi.getFileName())))
              .collect(Collectors.toList()));
    }
    return result;
  }

  /** Update status from QUEUED to RUNNING to display proper status */
  public void updateStatusToRunning(Validation validation) {

    // In case when validation was finihsed we need don't need to update the status
    Status newStatus;
    if (validation.hasFinished()) {
      newStatus = validation.getStatus();
    } else {
      newStatus = Status.RUNNING;
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

    log.info("Validaton {} change status to {} for {}", validation.getKey(), newStatus, STEP_TYPE);
    validationClient.update(validation.getKey(), validation);
  }
}
