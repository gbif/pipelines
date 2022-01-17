package org.gbif.pipelines.tasks;

import org.gbif.common.messaging.api.messages.PipelineBasedMessage;

public interface StepHandler<I extends PipelineBasedMessage, O extends PipelineBasedMessage> {

  Runnable createRunnable(I message);

  default O createOutgoingMessage(I message) {
    return null;
  }

  boolean isMessageCorrect(I message);
}
