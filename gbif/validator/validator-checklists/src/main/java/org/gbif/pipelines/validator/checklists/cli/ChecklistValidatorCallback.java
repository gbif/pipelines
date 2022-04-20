package org.gbif.pipelines.validator.checklists.cli;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
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
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.TermInfo;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesChecklistValidatorMessage} is received. */
@Slf4j
public class ChecklistValidatorCallback
    extends AbstractMessageCallback<PipelinesChecklistValidatorMessage> implements Closeable {

  // Use stepType as a String to keep validation api separate to gbif-api
  private static final String STEP_TYPE = "VALIDATOR_COLLECT_METRICS";

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

    this.checklistValidator = new ChecklistValidator(toValidatorConfiguration(config));
    this.messagePublisher = messagePublisher;
  }

  /** Creates a NeoConfiguration from the pipeline configuration. */
  private static ChecklistValidator.Configuration toValidatorConfiguration(
      ChecklistValidatorConfiguration config) {
    NeoConfiguration neoConfiguration = new NeoConfiguration();
    neoConfiguration.mappedMemory = config.neoMappedMemory;
    neoConfiguration.neoRepository = config.neoRepository;
    neoConfiguration.port = config.neoPort;
    neoConfiguration.batchSize = config.neoBatchSize;
    return ChecklistValidator.Configuration.builder()
        .neoConfiguration(neoConfiguration)
        .apiUrl(config.gbifApiUrl)
        .build();
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
  public void handleMessage(PipelinesChecklistValidatorMessage message) {
    Validation validation = validationClient.get(message.getDatasetUuid());

    if (validation != null) {
      validation = updateStatusToRunning(validation);
      if (!validation.hasFinished()) {
        validateArchive(validation);
      }
      // void send(Object message, String exchange, String routingKey, boolean persistent, String
      // correlationId, String replyTo)
      try {
        messagePublisher.replyToQueue(
            Boolean.TRUE, false, message.getCorrelationId(), message.getReplyTo());
      } catch (IOException e) {
        log.error("Can't replyToQueue {}, {}", message.getCorrelationId(), message.getReplyTo());
      }
    } else {
      log.error("Checklist validation started: {}", message);
    }
  }

  /** Performs the validation and update of validation data. */
  private void validateArchive(Validation validation) {
    try {
      log.info("Validating checklist archive: {}", validation.getKey());
      List<FileInfo> report =
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
  private void updateValidationFinished(Validation validation, List<FileInfo> report) {
    validation
        .getMetrics()
        .setFileInfos(mergeFilesInfo(validation.getMetrics().getFileInfos(), report));
    validationClient.update(validation);
    log.info("Checklist validation finished: {}", validation.getKey());
  }

  private static List<FileInfo> mergeFilesInfo(List<FileInfo> from, List<FileInfo> to) {

    // Find unique files info in both collections
    Map<String, FileInfo> fromMap = new HashMap<>();
    Optional.ofNullable(from).ifPresent(f -> f.forEach(x -> fromMap.put(x.getFileName(), x)));

    Map<String, FileInfo> toMap = new HashMap<>();
    Optional.ofNullable(to).ifPresent(f -> f.forEach(x -> toMap.put(x.getFileName(), x)));

    List<FileInfo> fromUnique =
        fromMap.entrySet().stream()
            .filter(es -> !toMap.containsKey(es.getKey()))
            .map(Entry::getValue)
            .collect(Collectors.toList());

    List<FileInfo> toUnique =
        toMap.entrySet().stream()
            .filter(es -> !fromMap.containsKey(es.getKey()))
            .map(Entry::getValue)
            .collect(Collectors.toList());

    // Merge non-unique files where "to" file is the main
    List<FileInfo> merged =
        toMap.entrySet().stream()
            .filter(es -> fromMap.containsKey(es.getKey()))
            .map(
                es -> {
                  FileInfo toFile = es.getValue();
                  FileInfo fromFile = fromMap.get(es.getKey());
                  toFile.setTerms(mergeTermsInfo(fromFile.getTerms(), toFile.getTerms()));
                  return toFile;
                })
            .collect(Collectors.toList());

    List<FileInfo> result = new ArrayList<>(fromUnique.size() + toUnique.size() + merged.size());
    result.addAll(fromUnique);
    result.addAll(toUnique);
    result.addAll(merged);

    return result;
  }

  /** Merge TermInfo where "to" is the main file and add only unique values "from" object */
  private static List<TermInfo> mergeTermsInfo(List<TermInfo> from, List<TermInfo> to) {

    Set<String> toSet = to.stream().map(TermInfo::getTerm).collect(Collectors.toSet());
    List<TermInfo> filtered =
        from.stream().filter(x -> !toSet.contains(x.getTerm())).collect(Collectors.toList());

    ArrayList<TermInfo> result = new ArrayList<>(to.size() + filtered.size());
    result.addAll(to);
    result.addAll(filtered);
    return result;
  }

  /** Update status from QUEUED to RUNNING to display proper status */
  public Validation updateStatusToRunning(Validation validation) {

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

    log.info("Validation {} change status to {} for {}", validation.getKey(), newStatus, STEP_TYPE);
    return validationClient.update(validation.getKey(), validation);
  }

  @Override
  public void close() {
    checklistValidator.close();
  }
}
