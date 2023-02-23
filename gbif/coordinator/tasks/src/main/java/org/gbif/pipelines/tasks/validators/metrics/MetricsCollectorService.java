package org.gbif.pipelines.tasks.validators.metrics;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.pipelines.validator.factory.ElasticsearchClientFactory;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage
 * } and perform conversion
 */
@Slf4j
public class MetricsCollectorService extends AbstractIdleService {

  private final MetricsCollectorConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;

  public MetricsCollectorService(MetricsCollectorConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-validator-metrics-collector with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());

    PipelinesHistoryClient historyClient =
        ServiceFactory.createPipelinesHistoryClient(config.stepConfig);

    ValidationWsClient validationClient =
        ServiceFactory.createValidationWsClient(config.stepConfig);

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(this.config, publisher, historyClient, validationClient);

    listener.listen(c.queueName, callback.getRouting(), c.poolSize, callback);
  }

  @SneakyThrows
  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    ElasticsearchClientFactory.getInstance().close();
    log.info("Stopping pipelines-validator-metrics-collector service");
  }
}
