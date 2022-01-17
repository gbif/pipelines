package org.gbif.pipelines.tasks.validator.validate;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;

@Slf4j
@Builder
public class DefaultValidator implements ArchiveValidator {

  private final ValidationWsClient validationClient;
  private final PipelinesArchiveValidatorMessage message;

  @Override
  public PipelineBasedMessage createOutgoingMessage() {
    throw new IllegalArgumentException(
        "File format " + message.getFileFormat() + " is not supported");
  }

  @Override
  public void validate() {
    log.info("File format {} is not supported!", message.getFileFormat());
    Validation validation = validationClient.get(message.getDatasetUuid());
    validation.setStatus(Status.FAILED);
    validationClient.update(validation);
  }
}
