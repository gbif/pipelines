package org.gbif.pipelines.crawler.abcd;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.pipelines.crawler.PipelineCallback;
import org.gbif.pipelines.crawler.xml.XmlToAvroCallback;
import org.gbif.pipelines.crawler.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 */
@Slf4j
public class AbcdToAvroCallback extends PipelineCallback<PipelinesAbcdMessage, PipelinesVerbatimMessage> {

  private final XmlToAvroConfiguration config;
  private final ExecutorService executor;

  public AbcdToAvroCallback(@NonNull CuratorFramework curator, @NonNull XmlToAvroConfiguration config,
      @NonNull ExecutorService executor, MessagePublisher publisher, PipelinesHistoryWsClient client) {
    super(StepType.ABCD_TO_VERBATIM, curator, publisher, client, config);
    this.config = config;
    this.executor = executor;
  }

  @Override
  protected Runnable createRunnable(PipelinesAbcdMessage message) {
    return XmlToAvroCallback.createRunnable(
        config,
        message.getDatasetUuid(),
        message.getAttempt().toString(),
        executor,
        XmlToAvroCallback.SKIP_RECORDS_CHECK
    );
  }

  @Override
  protected PipelinesVerbatimMessage createOutgoingMessage(PipelinesAbcdMessage message) {
    Objects.requireNonNull(message.getEndpointType(), "endpointType can't be NULL!");

    if (message.getPipelineSteps().isEmpty()) {
      message.setPipelineSteps(Sets.newHashSet(
          StepType.ABCD_TO_VERBATIM.name(),
          StepType.VERBATIM_TO_INTERPRETED.name(),
          StepType.INTERPRETED_TO_INDEX.name(),
          StepType.HDFS_VIEW.name()
      ));
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
  protected boolean isMessageCorrect(PipelinesAbcdMessage message) {
    return message.isModified();
  }
}
