package org.gbif.pipelines.crawler;

import org.gbif.common.messaging.api.messages.PipelineBasedMessage;

public interface PipelinesHandler<I extends PipelineBasedMessage, O extends PipelineBasedMessage> {

  Runnable createRunnable(I message);

  O createOutgoingMessage(I message);

  boolean isMessageCorrect(I message);
}
