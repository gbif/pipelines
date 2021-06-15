package org.gbif.pipelines.crawler.metrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesMetricsCollectedMessage;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/** Callback which is called when the {@link PipelinesIndexedMessage} is received. */
@Slf4j
public class MetricsCollectorCallback extends AbstractMessageCallback<PipelinesIndexedMessage>
    implements StepHandler<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage> {

  private final MetricsCollectorConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient client;

  public MetricsCollectorCallback(
      MetricsCollectorConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryWsClient client) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.client = client;
  }

  @Override
  public void handleMessage(PipelinesIndexedMessage message) {
    PipelinesCallback.<PipelinesIndexedMessage, PipelinesMetricsCollectedMessage>builder()
        .client(client)
        .config(config)
        .curator(curator)
        .stepType(StepType.VALIDATOR_COLLECT_METRICS)
        .isValidator(message.isValidator())
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesIndexedMessage message) {
    return true;
  }

  @Override
  public Runnable createRunnable(PipelinesIndexedMessage message) {
    return () -> {};
  }

  @Override
  public PipelinesMetricsCollectedMessage createOutgoingMessage(PipelinesIndexedMessage message) {
    return null;
  }
}
