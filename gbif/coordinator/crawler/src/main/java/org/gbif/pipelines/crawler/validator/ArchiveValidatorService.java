package org.gbif.pipelines.crawler.validator;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage
 * } and perform conversion
 */
@Slf4j
public class ArchiveValidatorService extends AbstractIdleService {

  private final ArchiveValidatorConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public ArchiveValidatorService(ArchiveValidatorConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-archive-validator service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();
    PipelinesHistoryWsClient client =
        c.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    String routingKey =
        new PipelinesArchiveValidatorMessage().setValidator(config.validatorOnly).getRoutingKey();
    listener.listen(
        c.queueName,
        routingKey,
        c.poolSize,
        new ArchiveValidatorCallback(this.config, publisher, curator, client));
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    log.info("Stopping pipelines-pipelines-archive-validator service");
  }
}
