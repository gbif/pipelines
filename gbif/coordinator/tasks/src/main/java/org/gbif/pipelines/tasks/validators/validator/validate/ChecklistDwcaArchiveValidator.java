package org.gbif.pipelines.tasks.validators.validator.validate;

import java.util.*;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
public class ChecklistDwcaArchiveValidator extends BaseDwcaArchiveValidator {

  @Builder
  public ChecklistDwcaArchiveValidator(
      ArchiveValidatorConfiguration config,
      ValidationWsClient validationClient,
      SchemaValidatorFactory schemaValidatorFactory,
      PipelinesArchiveValidatorMessage message) {
    super(config, validationClient, schemaValidatorFactory, message);
  }

  @Override
  @SneakyThrows
  public PipelinesChecklistValidatorMessage createOutgoingMessage() {
    PipelinesChecklistValidatorMessage m =
        new PipelinesChecklistValidatorMessage(
            message.getDatasetUuid(),
            message.getAttempt(),
            message.getPipelineSteps(),
            message.getExecutionId(),
            FileFormat.DWCA.name());
    return m;
  }
}
