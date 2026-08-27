package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildChecklistDwcaInputPath;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.UUID;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.ChecklistValidator;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
public class ChecklistDwcaArchiveValidator extends BaseDwcaArchiveValidator {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ChecklistValidator checklistValidator;

  @Builder
  public ChecklistDwcaArchiveValidator(
      ArchiveValidatorConfiguration config,
      ValidationWsClient validationClient,
      SchemaValidatorFactory schemaValidatorFactory,
      PipelinesArchiveValidatorMessage message,
      ChecklistValidator checklistValidator) {
    super(config, validationClient, schemaValidatorFactory, message);
    this.checklistValidator = checklistValidator;
  }

  @Override
  @SneakyThrows
  public Validation runValidations(Validation validation) {
    // DWCA validation
    validation = validateDwcaArchive(validation);
    validation.setStatus(Validation.Status.WAITING_FOR_CHECKLISTBANK);
    validation.setClbValidationMessage(OBJECT_MAPPER.writeValueAsString(message));

    Path archivePath =
        buildChecklistDwcaInputPath(
            config.archiveRepository, message.getDatasetUuid(), validation.getFile());
    UUID validationKey = validation.getKey();

    checklistValidator
        .submitValidation(archivePath, validationKey)
        .whenComplete(
            (datasetKey, throwable) -> {
              Validation currentValidation = validationClient.get(validationKey);
              if (currentValidation == null) {
                log.error("Couldn't find validation for {}", validationKey, throwable);
                throw new IllegalStateException("Couldn't find validation for " + validationKey);
              }

              if (throwable != null || datasetKey == null) {
                log.error(
                    "Error submitting CLB validation for {} and datasetKey received {}",
                    validationKey,
                    datasetKey,
                    throwable);
                currentValidation.setStatus(Validation.Status.FAILED);
                validationClient.update(currentValidation);
                return;
              }

              log.info(
                  "CLB archive validation submitted with dataset key {} for validation {}",
                  datasetKey,
                  validationKey);
              currentValidation.setClbDatasetKey(datasetKey);
              validationClient.update(currentValidation);
            });

    return validation;
  }

  @Override
  public PipelineBasedMessage createOutgoingMessage() {
    // we don't send a message because the validation is async and will be resumed when clb api
    // calls us back
    return null;
  }

  @Override
  public Validation.Status getFinalValidationStatus() {
    return Validation.Status.WAITING_FOR_CHECKLISTBANK;
  }
}
