package org.gbif.pipelines.crawler.dwca;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

/**
 * A service which listens to the  {@link org.gbif.common.messaging.api.messages.PipelinesDwcaMessage } and perform conversion
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
    StepConfiguration stepConfig = config.stepConfig;
    listener = new MessageListener(stepConfig.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(stepConfig.messaging.getConnectionParameters());
    curator = stepConfig.zooKeeper.getCuratorFramework();
    PipelinesHistoryWsClient client = stepConfig.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    listener.listen(stepConfig.queueName, stepConfig.poolSize, new DwcaToAvroCallback(this.config, publisher, curator, client));
  }

  @Override
  protected void shutDown() {
    publisher.close();
    listener.close();
    curator.close();
    log.info("Stopping pipelines-to-avro-from-dwca service");
  }
}
