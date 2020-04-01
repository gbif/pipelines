package org.gbif.pipelines.crawler.fragmenter;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.crawler.PipelineCallback;
import org.gbif.pipelines.crawler.indexing.IndexingConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Callback which is called when the {@link PipelinesInterpretedMessage} is received.
 * <p>
 * The main method is {@link FragmenterCallback#handleMessage}
 */
@Slf4j
@AllArgsConstructor
public class FragmenterCallback extends AbstractMessageCallback<PipelinesInterpretedMessage> {

  private static final StepType STEP = StepType.FRAGMENTER;

  @NonNull
  private final FragmenterConfiguration config;
  private final MessagePublisher publisher;
  @NonNull
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient historyClient;
  private final ExecutorService executor;

  /** Handles a MQ {@link PipelinesInterpretedMessage} message */
  @Override
  public void handleMessage(PipelinesInterpretedMessage message) {

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    try (MDCCloseable mdc1 = MDC.putCloseable("datasetId", datasetId.toString());
        MDCCloseable mdc2 = MDC.putCloseable("attempt", attempt.toString());
        MDCCloseable mdc3 = MDC.putCloseable("step", STEP.name())) {

      if (!isMessageCorrect(message)) {
        log.info("Skip the message, cause the runner is different or it wasn't modified, exit from handler");
        return;
      }

      log.info("Message handler began - {}", message);

      Set<String> steps = message.getPipelineSteps();
      Runnable runnable = createRunnable(message);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.builder()
          .incomingMessage(message)
          //.outgoingMessage()
          .curator(curator)
          .zkRootElementPath(STEP.getLabel())
          .pipelinesStepName(STEP)
          .publisher(publisher)
          .runnable(runnable)
          .historyClient(historyClient)
          .metricsSupplier(metricsSupplier(datasetId.toString(), attempt.toString()))
          .build()
          .handleMessage();

      log.info("Message handler ended - {}", message);

    }
  }

  /**
   * Main message processing logic, creates a terminal java process, which runs
   */
  private Runnable createRunnable(PipelinesInterpretedMessage message) {
    return () -> {
      throw new IllegalArgumentException("EMPTY METHOD!");
    };
  }

  /**
   * Only correct messages can be handled, by now is only messages with the same runner as runner in service config
   * {@link IndexingConfiguration#processRunner}
   */
  private boolean isMessageCorrect(PipelinesInterpretedMessage message) {
    return message.getOnlyForStep() == null || message.getOnlyForStep().equalsIgnoreCase(STEP.name());
  }

  private Supplier<List<PipelineStep.MetricInfo>> metricsSupplier(String datasetId, String attempt) {
    return () -> {
      String path = HdfsUtils.buildOutputPathAsString(config.repositoryPath, datasetId, attempt, config.metaFileName);
      return HdfsUtils.readMetricsFromMetaFile(config.hdfsSiteConfig, path);
    };
  }
}
