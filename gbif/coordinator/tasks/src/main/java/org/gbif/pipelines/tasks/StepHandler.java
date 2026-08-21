package org.gbif.pipelines.tasks;

import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.validator.api.Validation;

public interface StepHandler<In extends PipelineBasedMessage, Out extends PipelineBasedMessage> {

  Runnable createRunnable(In message);

  default Out createOutgoingMessage(In message) {
    return null;
  }

  boolean isMessageCorrect(In message);

  /**
   * Routing key to listen for MQ client, be aware it is not used anywhere by default, you need to
   * call the method manually
   */
  default String getRouting() {
    throw new PipelinesException("MQ routing key is not specified");
  }

  /** Only used in {@link ValidatorCallback} to set the status at the end of the process. */
  default Validation.Status getFinalValidationStatus(In message) {
    return Validation.Status.FINISHED;
  }
}
