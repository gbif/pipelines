package org.gbif.pipelines.crawler.abcd;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.crawler.PipelineCallback;
import org.gbif.pipelines.crawler.xml.XmlToAvroCallback;
import org.gbif.pipelines.crawler.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.common.utils.HdfsUtils.buildOutputPathAsString;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 * <p>
 * The main method is {@link AbcdToAvroCallback#handleMessage}
 */
@Slf4j
@AllArgsConstructor
public class AbcdToAvroCallback extends AbstractMessageCallback<PipelinesAbcdMessage> {

  private static final StepType STEP = StepType.ABCD_TO_VERBATIM;

  @NonNull
  private final CuratorFramework curator;
  @NonNull
  private final XmlToAvroConfiguration config;
  @NonNull
  private final ExecutorService executor;
  private final MessagePublisher publisher;
  private final PipelinesHistoryWsClient historyWsClient;

  /**
   * Handles a MQ {@link PipelinesAbcdMessage} message
   */
  @Override
  public void handleMessage(PipelinesAbcdMessage message) {

    UUID datasetId = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    try (MDCCloseable mdc1 = MDC.putCloseable("datasetId", datasetId.toString());
        MDCCloseable mdc2 = MDC.putCloseable("attempt", attempt.toString());
        MDCCloseable mdc3 = MDC.putCloseable("step", STEP.name())) {

      if (!message.isModified()) {
        log.info("Skip the message, cause it wasn't modified, exit from handler");
        return;
      }

      log.info("Message handler began - {}", message);

      if (message.getPipelineSteps().isEmpty()) {
        message.setPipelineSteps(Sets.newHashSet(
            StepType.ABCD_TO_VERBATIM.name(),
            StepType.VERBATIM_TO_INTERPRETED.name(),
            StepType.INTERPRETED_TO_INDEX.name(),
            StepType.HDFS_VIEW.name()
        ));
      }

      // Common variables
      Set<String> steps = message.getPipelineSteps();
      EndpointType endpointType = message.getEndpointType();
      Runnable runnable =
          XmlToAvroCallback.createRunnable(config, datasetId, attempt.toString(), endpointType, executor, XmlToAvroCallback.SKIP_RECORDS_CHECK);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.builder()
          .incomingMessage(message)
          .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps, endpointType))
          .curator(curator)
          .zkRootElementPath(STEP.getLabel())
          .pipelinesStepName(STEP)
          .publisher(publisher)
          .runnable(runnable)
          .historyWsClient(historyWsClient)
          .metricsSupplier(metricsSupplier(datasetId.toString(), attempt.toString()))
          .build()
          .handleMessage();

      log.info("Message handler ended - {}", message);
    }
  }

  private Supplier<List<PipelineStep.MetricInfo>> metricsSupplier(String datasetId, String attempt) {
    return () -> {
      String path = buildOutputPathAsString(config.repositoryPath, datasetId, attempt, config.metaFileName);
      return HdfsUtils.readMetricsFromMetaFile(config.hdfsSiteConfig, path);
    };
  }
}
