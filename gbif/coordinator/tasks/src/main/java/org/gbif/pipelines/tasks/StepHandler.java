package org.gbif.pipelines.tasks;

import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.common.PipelinesException;

public interface StepHandler<I extends PipelineBasedMessage, O extends PipelineBasedMessage> {

  Runnable createRunnable(I message);

  default O createOutgoingMessage(I message) {
    return null;
  }

  boolean isMessageCorrect(I message);

  /**
   * Routing key to listen for MQ client, be aware it is not used anywhere by default, you need to
   * call the method manually
   */
  default String getRouting() {
    throw new PipelinesException("MQ routing key is not specified");
  }
}
