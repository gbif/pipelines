package org.gbif.pipelines.crawler.fragmenter;

import java.util.concurrent.ExecutorService;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesFragmenterMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.PipelinesHandler;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;

import lombok.extern.slf4j.Slf4j;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 */
@Slf4j
public class FragmenterCallback extends AbstractMessageCallback<PipelinesInterpretedMessage>
    implements PipelinesHandler<PipelinesInterpretedMessage, PipelinesFragmenterMessage> {

  private static final StepType TYPE = StepType.FRAGMENTER;

  private final FragmenterConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient client;
  private final ExecutorService executor;

  public FragmenterCallback(FragmenterConfiguration config, MessagePublisher publisher,
      CuratorFramework curator, PipelinesHistoryWsClient client, ExecutorService executor) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.client = client;
    this.executor = executor;
  }

  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {
    PipelinesCallback.<PipelinesInterpretedMessage, PipelinesFragmenterMessage>builder()
        .client(client)
        .config(config)
        .curator(curator)
        .stepType(TYPE)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }


  @Override
  public Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> log.warn("Empty!");
  }

  @Override
  public PipelinesFragmenterMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesFragmenterMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps()
    );
  }

  @Override
  public boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    return message.getOnlyForStep() == null || message.getOnlyForStep().equalsIgnoreCase(TYPE.name());
  }
}
