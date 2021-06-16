package org.gbif.pipelines.crawler.metrics;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage
 * } and perform conversion
 */
@Slf4j
public class MetricsCollectorService extends AbstractIdleService {

  private final MetricsCollectorConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public MetricsCollectorService(MetricsCollectorConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-metrics-collector with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();
    PipelinesHistoryWsClient client =
        c.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    String routingKey =
        new PipelinesIndexedMessage().setValidator(config.validatorOnly).getRoutingKey();
    listener.listen(
        c.queueName,
        routingKey,
        c.poolSize,
        new MetricsCollectorCallback(this.config, publisher, curator, client));
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    log.info("Stopping pipelines-metrics-collector service");
  }
}
