package org.gbif.pipelines.crawler.fragmenter;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.pipelines.keygen.config.KeygenConfigFactory;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.client.Connection;

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
  private Connection hbaseConnection;

  public FragmenterService(FragmenterConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-fragmenter service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();
    executor = Executors.newFixedThreadPool(config.numberThreads);
    PipelinesHistoryWsClient client = c.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);
    KeygenConfig keygenConfig = KeygenConfigFactory.create(Paths.get(config.pipelinesConfig));

    FragmenterCallback callback =
        new FragmenterCallback(config, publisher, curator, client, executor, hbaseConnection, keygenConfig);
    listener.listen(c.queueName, c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    try {
      listener.close();
      publisher.close();
      curator.close();
      hbaseConnection.close();
      executor.shutdown();
      log.info("Stopping pipelines-fragmenter service");
    } catch (IOException ex) {
      log.warn("Couldn't close some resources during the exit - {}", ex.getMessage());
    }
  }
}
