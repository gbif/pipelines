package org.gbif.crawler.pipelines.abcd;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.crawler.pipelines.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * Service for the {@link AbcdToAvroCommand}.
 * <p>
 * This service listens to {@link org.gbif.common.messaging.api.messages.PipelinesXmlMessage}.
 */
public class AbcdToAvroService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AbcdToAvroService.class);

  private final XmlToAvroConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;
  private ExecutorService executor;

  public AbcdToAvroService(XmlToAvroConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started pipelines-to-avro-from-abcd service with parameters : {}", config);
    // create the listener.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    // creates a binding between the queue specified in the configuration and the exchange and routing key specified in
    // CrawlFinishedMessage
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();
    executor = Executors.newFixedThreadPool(config.xmlReaderParallelism);
    PipelinesHistoryWsClient historyWsClient = config.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    AbcdToAvroCallback callback = new AbcdToAvroCallback(config, publisher, curator, historyWsClient, executor);
    listener.listen(config.queueName, config.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    executor.shutdown();
    LOG.info("Stopping pipelines-to-avro-from-abcd service");
  }

}
