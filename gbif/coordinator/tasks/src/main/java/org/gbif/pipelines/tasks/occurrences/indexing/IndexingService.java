package org.gbif.pipelines.tasks.occurrences.indexing;

import com.google.common.util.concurrent.AbstractIdleService;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage }
 */
@Slf4j
public class IndexingService extends AbstractIdleService {

  private final IndexingConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CloseableHttpClient httpClient;
  private ExecutorService executor;

  public IndexingService(IndexingConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-occurrence-indexing service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    executor =
        config.standaloneNumberThreads == null
            ? null
            : Executors.newFixedThreadPool(config.standaloneNumberThreads);
    httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();

    PipelinesHistoryClient historyClient =
        ServiceFactory.createPipelinesHistoryClient(config.stepConfig);

    ValidationWsClient validationClient =
        ServiceFactory.createValidationWsClient(config.stepConfig);

    DatasetClient datasetClient = ServiceFactory.createDatasetClient(config.stepConfig);

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .httpClient(httpClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    listener.listen(c.queueName, callback.getRouting(), c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    executor.shutdown();
    try {
      httpClient.close();
    } catch (IOException e) {
      log.error("Can't close ES http client connection");
    }
    log.info("Stopping pipelines-occurrence-indexing service");
  }
}
