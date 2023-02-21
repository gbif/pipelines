package org.gbif.pipelines.tasks.verbatims.dwca;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/**
 * A service which listens to the {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage
 * } and perform conversion
 */
@Slf4j
public class DwcaToAvroService extends AbstractIdleService {

  private final DwcaToAvroConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;

  public DwcaToAvroService(DwcaToAvroConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-verbatim-to-avro-from-dwca service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());

    PipelinesHistoryClient historyClient =
        ServiceFactory.createPipelinesHistoryClient(config.stepConfig);

    ValidationWsClient validationClient =
        ServiceFactory.createValidationWsClient(config.stepConfig);

    DatasetClient datasetClient = ServiceFactory.createDatasetClient(config.stepConfig);

    DwcaToAvroCallback callback =
        DwcaToAvroCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    listener.listen(c.queueName, callback.getRouting(), c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    log.info("Stopping pipelines-verbatim-to-avro-from-dwca service");
  }
}
