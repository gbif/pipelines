package org.gbif.pipelines.crawler.fragmenter;

import com.google.common.util.concurrent.AbstractIdleService;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.client.Connection;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.ingest.java.utils.ConfigFactory;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
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
    PipelinesHistoryWsClient client =
        c.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);
    KeygenConfig keygenConfig =
        readConfig(c.hdfsSiteConfig, c.coreSiteConfig, config.pipelinesConfig);

    FragmenterCallback callback =
        new FragmenterCallback(
            config, publisher, curator, client, executor, hbaseConnection, keygenConfig);
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

  private KeygenConfig readConfig(
      String hdfsSiteConfig, String coreSiteConfig, String pipelinesConfig) {
    PipelinesConfig c =
        ConfigFactory.getInstance(
                hdfsSiteConfig, coreSiteConfig, pipelinesConfig, PipelinesConfig.class)
            .get();

    String zk = c.getKeygen().getZkConnectionString();
    zk = zk == null || zk.isEmpty() ? c.getZkConnectionString() : zk;

    return KeygenConfig.builder()
        .zkConnectionString(zk)
        .occurrenceTable(c.getKeygen().getOccurrenceTable())
        .lookupTable(c.getKeygen().getLookupTable())
        .counterTable(c.getKeygen().getCounterTable())
        .create();
  }
}
