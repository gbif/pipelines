package org.gbif.crawler.pipelines.dwca;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * A service which listens to the  {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage } and perform conversion
 */
public class DwcaToAvroService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToAvroService.class);
  private final DwcaToAvroConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public DwcaToAvroService(DwcaToAvroConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started pipelines-to-avro-from-dwca service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();
    PipelinesHistoryWsClient historyWsClient = config.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    listener.listen(config.queueName, config.poolSize, new DwcaToAvroCallback(config, publisher, curator, historyWsClient));
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    LOG.info("Stopping pipelines-to-avro-from-dwca service");
  }
}
