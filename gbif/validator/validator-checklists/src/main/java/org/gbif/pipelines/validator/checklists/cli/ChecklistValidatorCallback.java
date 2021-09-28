package org.gbif.pipelines.validator.checklists.cli;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.checklistbank.cli.common.NeoConfiguration;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.pipelines.validator.checklists.ChecklistValidator;
import org.gbif.pipelines.validator.checklists.cli.config.ChecklistValidatorConfiguration;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Callback which is called when the {@link PipelinesChecklistValidatorMessage} is received. */
@Slf4j
public class ChecklistValidatorCallback
    extends AbstractMessageCallback<PipelinesChecklistValidatorMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(ChecklistValidatorCallback.class);

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
      validateArchive(validation);
      // void send(Object message, String exchange, String routingKey, boolean persistent, String
      // correlationId, String replyTo)
      messagePublisher.replyToQueue(
          Boolean.TRUE, true, getContext().getCorrelationId(), getContext().getReplyTo());
    } else {
      LOG.error("Checklist validation started: {}", message);
    }
  }

  /** Performs the validation and update of validation data. */
  private void validateArchive(Validation validation) {
    try {
      LOG.info("Validating checklist archive: {}", validation.getKey());
      List<Metrics.FileInfo> report =
          checklistValidator.evaluate(
              buildDwcaInputPath(config.archiveRepository, validation.getKey()));
      updateValidationFinished(validation, report);
    } catch (Exception ex) {
      LOG.error("Error validating checklist", ex);
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
    if (validation.getMetrics().getFileInfos() == null) {
      validation.getMetrics().setFileInfos(report);
    } else {
      validation.getMetrics().getFileInfos().addAll(report);
    }
    validationClient.update(validation);
    LOG.info("Checklist validation finished: {}", validation.getKey());
  }
}
