package org.gbif.pipelines.crawler.interpret;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;

import com.google.common.util.concurrent.AbstractIdleService;
import lombok.extern.slf4j.Slf4j;

/**
 * A service which listens to the  {@link org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage } and perform interpretation
 */
@Slf4j
public class InterpretationService extends AbstractIdleService {

  private final InterpreterConfiguration config;
  private MessageListener listener;
  private MessagePublisher publisher;
  private CuratorFramework curator;
  private ExecutorService executor;

  public InterpretationService(InterpreterConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    log.info("Started pipelines-interpret-dataset service with parameters : {}", config);
    // Prefetch is one, since this is a long-running process.
    listener = new MessageListener(config.messaging.getConnectionParameters(), 1);
    publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    curator = config.zooKeeper.getCuratorFramework();
    executor = config.standaloneNumberThreads == null ? null : Executors.newFixedThreadPool(config.standaloneNumberThreads);
    PipelinesHistoryWsClient historyWsClient = config.registry.newRegistryInjector().getInstance(PipelinesHistoryWsClient.class);

    InterpretationCallback callback = new InterpretationCallback(config, publisher, curator, historyWsClient, executor);
    listener.listen(config.queueName, config.poolSize, callback);
  }

  @Override
  protected void shutDown() {
    listener.close();
    publisher.close();
    curator.close();
    executor.shutdown();
    log.info("Stopping pipelines-interpret-dataset service");
  }

}
