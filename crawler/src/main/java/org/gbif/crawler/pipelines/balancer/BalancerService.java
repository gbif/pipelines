package org.gbif.crawler.pipelines.balancer;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesBalancerMessage }
 */
public class BalancerService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(BalancerService.class);
  private final BalancerConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;

  public BalancerService(BalancerConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Started pipelines-balancer service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());

    listener.listen(config.queueName, config.poolSize, new BalancerCallback(config, publisher));
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    LOG.info("Stopping pipelines-balancer service");
  }
}
