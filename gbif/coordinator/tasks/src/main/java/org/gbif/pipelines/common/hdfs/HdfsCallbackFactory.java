package org.gbif.pipelines.common.hdfs;

import java.util.concurrent.ExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretationMessage;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

@FunctionalInterface
public interface HdfsCallbackFactory<
    I extends PipelinesInterpretationMessage, B extends PipelineBasedMessage> {
  HdfsViewCallback<I, B> createCallBack(
      HdfsViewConfiguration config,
      MessagePublisher publisher,
      CuratorFramework curator,
      PipelinesHistoryClient historyClient,
      ExecutorService executor);
}
