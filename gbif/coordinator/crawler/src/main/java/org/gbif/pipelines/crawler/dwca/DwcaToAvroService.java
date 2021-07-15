package org.gbif.pipelines.crawler.dwca;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage
 * } and perform conversion
 */
@Slf4j
public class DwcaToAvroService extends AbstractIdleService {

  private final DwcaToAvroConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public DwcaToAvroService(DwcaToAvroConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-to-avro-from-dwca service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();

    PipelinesHistoryClient historyClient =
        new ClientBuilder()
            .withUrl(config.stepConfig.registry.wsUrl)
            .withCredentials(config.stepConfig.registry.user, config.stepConfig.registry.password)
            .withFormEncoder()
            .build(PipelinesHistoryClient.class);

    ValidationWsClient validationClient =
        new ClientBuilder()
            .withUrl(config.stepConfig.registry.wsUrl)
            .withCredentials(config.stepConfig.registry.user, config.stepConfig.registry.password)
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .build(ValidationWsClient.class);

    String routingKey =
        new PipelinesDwcaMessage().setValidator(config.validatorOnly).getRoutingKey();
    listener.listen(
        c.queueName,
        routingKey,
        c.poolSize,
        new DwcaToAvroCallback(this.config, publisher, curator, historyClient, validationClient));
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    log.info("Stopping pipelines-to-avro-from-dwca service");
  }
}
