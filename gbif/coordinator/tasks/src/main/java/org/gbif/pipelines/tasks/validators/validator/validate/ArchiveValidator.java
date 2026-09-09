package org.gbif.pipelines.tasks.validators.validator.validate;

import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.validator.api.Validation;

public interface ArchiveValidator {

  PipelineBasedMessage createOutgoingMessage();

  default void validate() {
    // NOP
  }

  default Validation.Status getFinalValidationStatus() {
    return Validation.Status.FINISHED;
  }
}
