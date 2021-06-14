package org.gbif.pipelines.crawler.validator;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/** Callback which is called when the {@link PipelinesDwcaMessage} is received. */
@Slf4j
public class ArchiveValidatorCallback extends AbstractMessageCallback<PipelinesDwcaMessage>
    implements StepHandler<PipelinesDwcaMessage, PipelinesVerbatimMessage> {

  private final ArchiveValidatorConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient client;

  public ArchiveValidatorCallback(
      ArchiveValidatorConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryWsClient client) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.client = client;
  }

  @Override
  public void handleMessage(PipelinesDwcaMessage message) {
    PipelinesCallback.<PipelinesDwcaMessage, PipelinesVerbatimMessage>builder()
        .client(client)
        .config(config)
        .curator(curator)
        .stepType(null) // Add new type
        .isValidator(message.isValidator())
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public boolean isMessageCorrect(PipelinesDwcaMessage message) {
    return true;
  }

  @Override
  public Runnable createRunnable(PipelinesDwcaMessage message) {
    return () -> {
      log.info("EMPTY!");
    };
  }

  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesDwcaMessage message) {
    return null;
  }
}
