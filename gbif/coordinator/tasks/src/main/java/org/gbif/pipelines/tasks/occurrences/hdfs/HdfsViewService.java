package org.gbif.pipelines.tasks.occurrences.hdfs;

import com.google.common.util.concurrent.AbstractIdleService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.hdfs.CommonHdfsViewCallback;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.pipelines.tasks.events.hdfs.HdfsViewCallback;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
@Slf4j
public class HdfsViewService extends AbstractIdleService {

  private final HdfsViewConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;
  private ExecutorService executor;

  public HdfsViewService(HdfsViewConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-hdfs-view service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();
    executor =
        config.standaloneNumberThreads == null
            ? null
            : Executors.newFixedThreadPool(config.standaloneNumberThreads);

    PipelinesHistoryClient historyClient =
        ServiceFactory.createPipelinesHistoryClient(config.stepConfig);

    HdfsViewCallback callback =
        HdfsViewCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .commonHdfsViewCallback(CommonHdfsViewCallback.create(config, executor))
            .build();

    String routingKey =
        new PipelinesInterpretedMessage().setRunner(config.processRunner).getRoutingKey();
    listener.listen(c.queueName, routingKey, c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    executor.shutdown();
    log.info("Stopping pipelines-hdfs-view service");
  }
}
