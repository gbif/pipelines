package org.gbif.pipelines.tasks.camtrapdp;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

/**
 * A service which listens to the {@link
 * org.gbif.common.messaging.api.messages.PipelinesCamtrapDpMessage} and performs a conversion from
 * CamtrapDP to DwC-A.
 */
@Slf4j
public class CamtrapDpToDwcaService extends AbstractIdleService {

  private final CamtrapToDwcaConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;

  public CamtrapDpToDwcaService(CamtrapToDwcaConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-camtrapdp-to-dwca service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());

    DatasetClient datasetClient =
        new ClientBuilder()
            .withUrl(config.gbifApiWsUrl)
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .build(DatasetClient.class);

    CamtrapDpToDwcaCallback callback =
        new CamtrapDpToDwcaCallback(config, publisher, datasetClient);

    listener.listen(c.queueName, callback.getRouting(), c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    log.info("Stopping pipelines-camtrapdp-to-dwca service");
  }
}
