package org.gbif.pipelines.tasks.events.interpretation;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.ServiceFactory;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;

/**
 * A service which listens to the {@link PipelinesEventsMessage } to start the events
 * interpretation.
 */
@Slf4j
public class EventsInterpretationService extends AbstractIdleService {

  private final EventsInterpretationConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;

  public EventsInterpretationService(EventsInterpretationConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-event-interpretation service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());

    PipelinesHistoryClient historyClient =
        ServiceFactory.createPipelinesHistoryClient(config.stepConfig);

    DatasetClient datasetClient = ServiceFactory.createDatasetClient(config.stepConfig);

    EventsInterpretationCallback callback =
        EventsInterpretationCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .datasetClient(datasetClient)
            .hdfsConfigs(
                HdfsConfigs.create(
                    config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig))
            .build();

    listener.listen(c.queueName, callback.getRouting(), c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    log.info("Stopping pipelines-event-interpretation service");
  }
}
