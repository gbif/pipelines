package org.gbif.pipelines.tasks.events.hdfs;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsHdfsViewBuiltMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.pipelines.common.hdfs.CommonHdfsViewCallback;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when an instance {@link PipelinesInterpretationMessage} is received. */
@Slf4j
@Builder
public class HdfsViewCallback extends AbstractMessageCallback<PipelinesEventsInterpretedMessage>
    implements StepHandler<PipelinesEventsInterpretedMessage, PipelinesEventsHdfsViewBuiltMessage> {

  private final HdfsViewConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryClient historyClient;
  private final CommonHdfsViewCallback commonHdfsViewCallback;

  @Override
  public void handleMessage(PipelinesEventsInterpretedMessage message) {
    PipelinesCallback
        .<PipelinesEventsInterpretedMessage, PipelinesEventsHdfsViewBuiltMessage>builder()
        .historyClient(historyClient)
        .config(config)
        .curator(curator)
        .stepType(config.stepType)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    return new PipelinesEventsInterpretedMessage().setRunner("*").getRoutingKey();
  }

  /** Main message processing logic, creates a terminal java process, which runs */
  @Override
  public Runnable createRunnable(PipelinesEventsInterpretedMessage message) {
    return commonHdfsViewCallback.createRunnable(message);
  }

  @Override
  public PipelinesEventsHdfsViewBuiltMessage createOutgoingMessage(
      PipelinesEventsInterpretedMessage message) {
    return new PipelinesEventsHdfsViewBuiltMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps());
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link HdfsViewConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesEventsInterpretedMessage message) {
    return commonHdfsViewCallback.isMessageCorrect(message);
  }
}
