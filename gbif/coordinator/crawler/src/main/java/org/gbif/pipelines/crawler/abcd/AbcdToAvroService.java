package org.gbif.pipelines.crawler.abcd;

import com.google.common.util.concurrent.AbstractIdleService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClients;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.crawler.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/**
 * Service for the {@link AbcdToAvroCommand}.
 *
 * <p>This service listens to {@link org.gbif.common.messaging.api.messages.PipelinesXmlMessage}.
 */
@Slf4j
public class AbcdToAvroService extends AbstractIdleService {

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
    log.info("Started pipelines-to-avro-from-abcd service with parameters : {}", config);
    // create the listener.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    // creates a binding between the queue specified in the configuration and the exchange and
    // routing key specified in
    // CrawlFinishedMessage
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();
    executor = Executors.newFixedThreadPool(config.xmlReaderParallelism);
    PipelinesHistoryWsClient client =
        c.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    HttpClient httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();

    AbcdToAvroCallback callback =
        new AbcdToAvroCallback(curator, config, executor, publisher, client, httpClient);
    listener.listen(c.queueName, c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    executor.shutdown();
    log.info("Stopping pipelines-to-avro-from-abcd service");
  }
}
