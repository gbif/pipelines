package org.gbif.pipelines.validator.checklists.cli;

import com.google.common.util.concurrent.AbstractIdleService;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.pipelines.validator.checklists.cli.config.ChecklistValidatorConfiguration;
import org.gbif.pipelines.validator.checklists.cli.config.RegistryConfiguration;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.client.ClientContract;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.cloud.openfeign.annotation.PathVariableParameterProcessor;
import org.springframework.cloud.openfeign.annotation.QueryMapParameterProcessor;
import org.springframework.cloud.openfeign.annotation.RequestHeaderParameterProcessor;
import org.springframework.cloud.openfeign.annotation.RequestParamParameterProcessor;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage
 * } and perform conversion
 */
@Slf4j
public class ChecklistValidatorService extends AbstractIdleService {

  private final ChecklistValidatorConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;

  public ChecklistValidatorService(ChecklistValidatorConfiguration config) {
    this.config = config;
  }

  public static ValidationWsClient createValidationWsClient(
      RegistryConfiguration registryConfiguration) {
    return new ClientBuilder()
        .withUrl(registryConfiguration.wsUrl)
        .withCredentials(registryConfiguration.user, registryConfiguration.password)
        .withObjectMapper(
            JacksonJsonObjectMapperProvider.addBuilderSupport(
                JacksonJsonObjectMapperProvider.getDefaultObjectMapper()))
        .withClientContract(
            ClientContract.withProcessors(
                new PathVariableParameterProcessor(),
                new RequestParamParameterProcessor(),
                new RequestHeaderParameterProcessor(),
                new QueryMapParameterProcessor()))
        .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
        .build(ValidationWsClient.class);
  }

  @Override
  protected void startUp() throws Exception {
    log.info(
        "Started pipelines-validator-checklist-validator service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());

    ValidationWsClient validationClient = createValidationWsClient(config.registry);

    String routingKey = new PipelinesChecklistValidatorMessage().getRoutingKey();
    listener.listen(
        config.queueName,
        routingKey,
        config.poolSize,
        new ChecklistValidatorCallback(this.config, validationClient, publisher));
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    log.info("Stopping pipelines-validator-checklist-validator service");
  }
}
