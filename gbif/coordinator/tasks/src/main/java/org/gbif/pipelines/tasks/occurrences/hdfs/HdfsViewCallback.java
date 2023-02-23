package org.gbif.pipelines.tasks.occurrences.hdfs;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.hdfs.CommonHdfsViewCallback;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when an instance {@link PipelinesInterpretationMessage} is received. */
@Slf4j
@Builder
public class HdfsViewCallback extends AbstractMessageCallback<PipelinesInterpretedMessage>
    implements StepHandler<PipelinesInterpretedMessage, PipelinesHdfsViewMessage> {

  protected final HdfsViewConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;
  private final CommonHdfsViewCallback commonHdfsViewCallback;

  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {
    PipelinesCallback.<PipelinesInterpretedMessage, PipelinesHdfsViewMessage>builder()
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .config(config)
        .stepType(config.stepType)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    return new PipelinesInterpretedMessage().setRunner(config.processRunner).getRoutingKey();
  }

  /** Main message processing logic, creates a terminal java process, which runs */
  @Override
  public Runnable createRunnable(PipelinesInterpretedMessage message) {
    return commonHdfsViewCallback.createRunnable(message);
  }

  @Override
  public PipelinesHdfsViewMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesHdfsViewMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps(), null, null);
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in
   * service config {@link HdfsViewConfiguration#processRunner}
   */
  @Override
  public boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    return commonHdfsViewCallback.isMessageCorrect(message);
  }
}
