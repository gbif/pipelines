package org.gbif.pipelines.tasks.occurrences.hdfs;

import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesHdfsViewBuiltMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.hdfs.HdfsViewCallback;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** Callback which is called when the {@link PipelinesInterpretedMessage} is received. */
@Slf4j
public class OccurrenceHdfsViewCallback
    extends HdfsViewCallback<PipelinesInterpretedMessage, PipelinesHdfsViewBuiltMessage> {

  public OccurrenceHdfsViewCallback(
      HdfsViewConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ExecutorService executor) {
    super(config, publisher, curator, historyClient, executor);
  }

  @Override
  public PipelinesHdfsViewBuiltMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesHdfsViewBuiltMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps());
  }

  public static OccurrenceHdfsViewCallback create(
      HdfsViewConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ExecutorService executor) {
    return new OccurrenceHdfsViewCallback(config, publisher, curator, historyClient, executor);
  }

  @Override
  public String routingKey() {
    return new PipelinesHdfsViewBuiltMessage().setRunner(config.processRunner).getRoutingKey();
  }
}
