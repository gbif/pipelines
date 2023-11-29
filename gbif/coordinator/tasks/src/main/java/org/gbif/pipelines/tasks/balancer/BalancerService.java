package org.gbif.pipelines.tasks.balancer;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesBalancerMessage }
 */
@Slf4j
public class BalancerService extends AbstractIdleService {

  private final BalancerConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;

  public BalancerService(BalancerConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-balancer service");
    StepConfiguration stepConfig = config.stepConfig;

    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(stepConfig.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(stepConfig.messaging.getConnectionParameters());

    listener.listen(
        stepConfig.queueName, stepConfig.poolSize, new BalancerCallback(config, publisher));
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    log.info("Stopping pipelines-balancer service");
  }
}
