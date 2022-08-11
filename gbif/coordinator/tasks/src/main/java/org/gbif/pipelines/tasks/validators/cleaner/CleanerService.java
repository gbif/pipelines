package org.gbif.pipelines.tasks.validators.cleaner;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.MessageListener;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.validator.ws.client.ValidationWsClient;

/**
 * Service for the {@link CleanerCommand}.
 *
 * <p>This service listens to {@link
 * org.gbif.common.messaging.api.messages.PipelinesCleanerMessage}.
 */
@Slf4j
public class CleanerService extends AbstractIdleService {

  private final CleanerConfiguration config;
  private MessageListener listener;
  private CuratorFramework curator;

  public CleanerService(CleanerConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-validator-cleaner service with parameters : {}", config);
    // create the listener.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    ValidationWsClient validationClient =
        ServiceFactory.createValidationWsClient(config.stepConfig);
    curator = c.zooKeeper.getCuratorFramework();

    CleanerCallback callback = new CleanerCallback(config, validationClient, curator);
    listener.listen(c.queueName, callback.getRouting(), c.poolSize, callback);
  }

  @Override
  protected void shutDown() throws Exception {
    log.info("Stopping pipelines-validator-cleaner service");
    curator.close();
  }
}
