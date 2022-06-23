package org.gbif.pipelines.tasks.events.hdfs;

import java.util.concurrent.ExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsHdfsViewBuiltMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.pipelines.common.hdfs.HdfsViewCallback;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Events HDFS View Callback. */
public class EventsOccurrenceHdfsViewCallback
    extends HdfsViewCallback<
        PipelinesEventsInterpretedMessage, PipelinesEventsHdfsViewBuiltMessage> {

  public EventsOccurrenceHdfsViewCallback(
      HdfsViewConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ExecutorService executor) {
    super(config, publisher, curator, historyClient, executor);
  }

  @Override
  public PipelinesEventsHdfsViewBuiltMessage createOutgoingMessage(
      PipelinesEventsInterpretedMessage message) {
    return new PipelinesEventsHdfsViewBuiltMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps());
  }

  @Override
  public String routingKey() {
    return new PipelinesEventsHdfsViewBuiltMessage()
        .setRunner(config.processRunner)
        .getRoutingKey();
  }
}
