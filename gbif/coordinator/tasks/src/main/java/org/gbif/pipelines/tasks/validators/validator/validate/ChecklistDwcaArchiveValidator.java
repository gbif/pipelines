package org.gbif.pipelines.tasks.validators.validator.validate;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Paths;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.checklists.ChecklistValidator;
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
  public Validation runValidations(Validation validation) {
    // DWCA validation
    validation = validateDwcaArchive(validation);

    try {
      int datasetKey =
          checklistValidator.submitValidation(
              Paths.get(
                  buildDwcaInputPath(config.archiveRepository, message.getDatasetUuid())
                      .toString()),
              validation.getKey());

      log.info("Submitted DWCA archive validation with dataset key {}", datasetKey);

      validation.setClbDatasetKey(datasetKey);
      validation.setClbValidationMessage(OBJECT_MAPPER.writeValueAsString(message));
    } catch (Exception ex) {
      log.error("Error validating Checklist DWCA archive", ex);
      validation.setStatus(Validation.Status.FAILED);
    }

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
