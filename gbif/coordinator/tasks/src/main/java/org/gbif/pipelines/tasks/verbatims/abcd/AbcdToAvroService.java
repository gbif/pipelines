package org.gbif.pipelines.tasks.verbatims.abcd;

import com.google.common.util.concurrent.AbstractIdleService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClients;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroCallback;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

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
  private ExecutorService executor;

  public AbcdToAvroService(XmlToAvroConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-verbatim-to-avro-from-abcd service with parameters : {}", config);
    // create the listener.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    // creates a binding between the queue specified in the configuration and the exchange and
    // routing key specified in
    // CrawlFinishedMessage
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    executor = Executors.newFixedThreadPool(config.xmlReaderParallelism);

    PipelinesHistoryClient historyClient =
        ServiceFactory.createPipelinesHistoryClient(config.stepConfig);

    ValidationWsClient validationClient =
        ServiceFactory.createValidationWsClient(config.stepConfig);

    DatasetClient datasetClient = ServiceFactory.createDatasetClient(config.stepConfig);

    HttpClient httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();

    AbcdToAvroCallback callback =
        AbcdToAvroCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .callback(
                XmlToAvroCallback.builder()
                    .config(config)
                    .publisher(publisher)
                    .historyClient(historyClient)
                    .validationClient(validationClient)
                    .executor(executor)
                    .httpClient(httpClient)
                    .datasetClient(datasetClient)
                    .build())
            .build();

    listener.listen(c.queueName, c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    executor.shutdown();
    log.info("Stopping pipelines-verbatim-to-avro-from-abcd service");
  }
}
