package org.gbif.pipelines.tasks.events.indexing;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.configs.StepConfiguration;

/** A service which listens to the {@link PipelinesInterpretedMessage } */
@Slf4j
public class EventsIndexingService extends AbstractIdleService {

  private final EventsIndexingConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;

  public EventsIndexingService(EventsIndexingConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-event-indexing service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    StepConfiguration c = config.stepConfig;
    listener = new MessageListener(c.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(c.messaging.getConnectionParameters());
    curator = c.zooKeeper.getCuratorFramework();

    EventsIndexingCallback callback = new EventsIndexingCallback(config, publisher, curator);

    String routingKey = new PipelinesEventsInterpretedMessage().getRoutingKey();
    listener.listen(c.queueName, routingKey, c.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    log.info("Stopping pipelines-event-indexing service");
  }
}
