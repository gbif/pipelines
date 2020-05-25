package org.gbif.pipelines.crawler.abcd;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.client.HttpClient;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.crawler.xml.XmlToAvroCallback;
import org.gbif.pipelines.crawler.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 */
@Slf4j
public class AbcdToAvroCallback extends AbstractMessageCallback<PipelinesAbcdMessage>
    implements StepHandler<PipelinesAbcdMessage, PipelinesVerbatimMessage> {

  private final CuratorFramework curator;
  private final XmlToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryWsClient client;
  private final XmlToAvroCallback callback;

  public AbcdToAvroCallback(
      CuratorFramework curator,
      XmlToAvroConfiguration config,
      ExecutorService executor,
      MessagePublisher publisher,
      PipelinesHistoryWsClient client,
      HttpClient httpClient) {
    this.curator = curator;
    this.config = config;
    this.publisher = publisher;
    this.client = client;
    this.callback = new XmlToAvroCallback(config, publisher, curator, client, executor, httpClient);
  }

  @Override
  public void handleMessage(PipelinesAbcdMessage message) {
    PipelinesCallback.<PipelinesAbcdMessage, PipelinesVerbatimMessage>builder()
        .client(client)
        .config(config)
        .curator(curator)
        .stepType(StepType.ABCD_TO_VERBATIM)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public Runnable createRunnable(PipelinesAbcdMessage message) {
    return callback.createRunnable(
        message.getDatasetUuid(),
        message.getAttempt().toString(),
        XmlToAvroCallback.SKIP_RECORDS_CHECK);
  }

  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesAbcdMessage message) {
    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(new HashSet<>(Arrays.asList(
          StepType.ABCD_TO_VERBATIM.name(),
          StepType.VERBATIM_TO_INTERPRETED.name(),
          StepType.INTERPRETED_TO_INDEX.name(),
          StepType.HDFS_VIEW.name(),
          StepType.FRAGMENTER.name()
      )));
    }

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        config.interpretTypes,
        message.getPipelineSteps(),
        message.getEndpointType()
    );
  }

  @Override
  public boolean isMessageCorrect(PipelinesAbcdMessage message) {
    return message.isModified();
  }
}
