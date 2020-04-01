package org.gbif.pipelines.crawler.fragmenter;

import java.util.concurrent.ExecutorService;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewBuiltMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.crawler.PipelineCallback;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 */
@Slf4j
public class FragmenterCallback extends PipelineCallback<PipelinesInterpretedMessage, PipelinesHdfsViewBuiltMessage> {

  private final FragmenterConfiguration config;
  private final ExecutorService executor;

  public FragmenterCallback(FragmenterConfiguration config, MessagePublisher publisher, CuratorFramework curator,
      PipelinesHistoryWsClient client, @NonNull ExecutorService executor) {
    super(StepType.FRAGMENTER, curator, publisher, client, config);
    this.config = config;
    this.executor = executor;
  }


  @Override
  protected Runnable createRunnable(PipelinesInterpretedMessage message) {
    throw new UnsupportedOperationException("EMPTY!");
  }

  @Override
  protected PipelinesHdfsViewBuiltMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    throw new UnsupportedOperationException("EMPTY!");
  }

  @Override
  protected boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    throw new UnsupportedOperationException("EMPTY!");
  }
}
