package org.gbif.pipelines.crawler.fragmenter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
@Slf4j
public class FragmenterService extends AbstractIdleService {

  private final FragmenterConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;
  private ExecutorService executor;

  public FragmenterService(FragmenterConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-fragmenter service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration stepConfig = config.stepConfig;
    listener = new MessageListener(stepConfig.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(stepConfig.messaging.getConnectionParameters());
    curator = stepConfig.zooKeeper.getCuratorFramework();
    executor = Executors.newFixedThreadPool(config.numberThreads);
    PipelinesHistoryWsClient client = stepConfig.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    FragmenterCallback callback = new FragmenterCallback(config, publisher, curator, client, executor);
    listener.listen(stepConfig.queueName, stepConfig.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    executor.shutdown();
    log.info("Stopping pipelines-fragmenter service");
  }
}
