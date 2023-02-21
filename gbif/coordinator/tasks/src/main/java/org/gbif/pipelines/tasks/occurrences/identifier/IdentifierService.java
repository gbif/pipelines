package org.gbif.pipelines.tasks.occurrences.identifier;

import com.google.common.util.concurrent.AbstractIdleService;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/** A service which listens to the {@link PipelinesVerbatimMessage } and perform interpretation */
@Slf4j
public class IdentifierService extends AbstractIdleService {

  private final IdentifierConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CloseableHttpClient httpClient;

  public IdentifierService(IdentifierConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info(
        "Started pipelines-occurrence-identifier dataset service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());

    PipelinesHistoryClient historyClient =
        ServiceFactory.createPipelinesHistoryClient(config.stepConfig);

    DatasetClient datasetClient = ServiceFactory.createDatasetClient(config.stepConfig);

    httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();

    IdentifierCallback callback =
        IdentifierCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .httpClient(httpClient)
            .datasetClient(datasetClient)
            .build();

    listener.listen(c.queueName, callback.getRouting(), c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    try {
      httpClient.close();
    } catch (IOException e) {
      log.error("Can't close ES http client connection");
    }
    log.info("Stopping pipelines-occurrence-identifier service");
  }
}
