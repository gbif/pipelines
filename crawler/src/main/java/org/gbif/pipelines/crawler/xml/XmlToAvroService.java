package org.gbif.pipelines.crawler.xml;

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
 * Service for the {@link XmlToAvroCommand}.
 * <p>
 * This service listens to {@link org.gbif.common.messaging.api.messages.PipelinesXmlMessage}.
 */
@Slf4j
public class XmlToAvroService extends AbstractIdleService {

  private final XmlToAvroConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;
  private ExecutorService executor;

  public XmlToAvroService(XmlToAvroConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-to-avro-from-xml service with parameters : {}", config);
    // create the listener.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    // creates a binding between the queue specified in the configuration and the exchange and routing key specified in
    // CrawlFinishedMessage
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();
    executor = Executors.newFixedThreadPool(config.xmlReaderParallelism);
    PipelinesHistoryWsClient client = c.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    XmlToAvroCallback callback = new XmlToAvroCallback(config, publisher, curator, client, executor);
    listener.listen(c.queueName, c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    executor.shutdown();
    log.info("Stopping pipelines-to-avro-from-xml service");
  }

}
