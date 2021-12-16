package org.gbif.pipelines.tasks.cleaner;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.messages.PipelinesCleanerMessage;
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

  public CleanerService(CleanerConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started validaton-cleaner service with parameters : {}", config);
    // create the listener.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    ValidationWsClient validationClient =
        ServiceFactory.createValidationWsClient(config.stepConfig);

    String routingKey =
        new PipelinesCleanerMessage().setValidator(config.validatorOnly).getRoutingKey();

    CleanerCallback callback = new CleanerCallback(config, validationClient);
    listener.listen(c.queueName, routingKey, c.poolSize, callback);
  }

  @Override
  protected void shutDown() throws Exception {
    log.info("Stopping validaton-cleaner service");
  }
}
