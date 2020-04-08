package org.gbif.pipelines.crawler.fragmenter;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesFragmenterMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.crawler.PipelinesCallback;
import org.gbif.pipelines.crawler.StepHandler;
import org.gbif.pipelines.fragmenter.FragmentPersister;
import org.gbif.pipelines.fragmenter.strategy.DwcaStrategy;
import org.gbif.pipelines.fragmenter.strategy.Strategy;
import org.gbif.pipelines.fragmenter.strategy.XmlStrategy;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.client.Connection;

import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.utils.PathUtil.buildDwcaInputPath;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 */
@Slf4j
public class FragmenterCallback extends AbstractMessageCallback<PipelinesInterpretedMessage>
    implements StepHandler<PipelinesInterpretedMessage, PipelinesFragmenterMessage> {

  private static final StepType TYPE = StepType.FRAGMENTER;

  private final FragmenterConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient client;
  private final ExecutorService executor;
  private final Connection hbaseConnection;
  private final KeygenConfig keygenConfig;

  public FragmenterCallback(FragmenterConfiguration config, MessagePublisher publisher, CuratorFramework curator,
      PipelinesHistoryWsClient client, ExecutorService executor, Connection hbaseConnection,
      KeygenConfig keygenConfig) {
    this.config = config;
    this.publisher = publisher;
    this.curator = curator;
    this.client = client;
    this.executor = executor;
    this.hbaseConnection = hbaseConnection;
    this.keygenConfig = keygenConfig;
  }

  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {
    PipelinesCallback.<PipelinesInterpretedMessage, PipelinesFragmenterMessage>builder()
        .client(client)
        .config(config)
        .curator(curator)
        .stepType(TYPE)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }


  @Override
  public Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {

      UUID datasetId = message.getDatasetUuid();

      Strategy strategy;
      Path pathToArchive;

      if (message.getEndpointType().equals(EndpointType.DWC_ARCHIVE)) {
        strategy = DwcaStrategy.create();
        pathToArchive = buildDwcaInputPath(config.dwcaArchiveRepository, datasetId);
      } else {
        strategy = XmlStrategy.create();
        pathToArchive = null;
      }

      long result = FragmentPersister.builder()
          .strategy(strategy)
          .endpointType(message.getEndpointType())
          .datasetKey(datasetId.toString())
          .attempt(message.getAttempt())
          .tableName(config.hbaseFragmentsTable)
          .hbaseConnection(hbaseConnection)
          .executor(executor)
          .useOccurrenceId(true)
          .useTriplet(true)
          .pathToArchive(pathToArchive)
          .keygenConfig(keygenConfig)
          .build()
          .persist();

      log.info("Result - {} records", result);
    };
  }

  @Override
  public PipelinesFragmenterMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesFragmenterMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps()
    );
  }

  @Override
  public boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    return message.getOnlyForStep() == null || message.getOnlyForStep().equalsIgnoreCase(TYPE.name());
  }
}
