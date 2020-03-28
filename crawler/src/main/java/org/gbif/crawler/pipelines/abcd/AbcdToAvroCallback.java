package org.gbif.crawler.pipelines.abcd;

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
import org.gbif.crawler.common.utils.HdfsUtils;
import org.gbif.crawler.pipelines.PipelineCallback;
import org.gbif.crawler.pipelines.xml.XmlToAvroCallback;
import org.gbif.crawler.pipelines.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import com.google.common.collect.Sets;

import static org.gbif.crawler.common.utils.HdfsUtils.buildOutputPathAsString;
import static org.gbif.crawler.pipelines.xml.XmlToAvroCallback.SKIP_RECORDS_CHECK;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Call back which is called when the {@link PipelinesXmlMessage} is received.
 * <p>
 * The main method is {@link AbcdToAvroCallback#handleMessage}
 */
public class AbcdToAvroCallback extends AbstractMessageCallback<PipelinesAbcdMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(AbcdToAvroCallback.class);
  private static final StepType STEP = StepType.ABCD_TO_VERBATIM;

  private final XmlToAvroConfiguration config;
  private final MessagePublisher publisher;
  private final CuratorFramework curator;
  private final PipelinesHistoryWsClient historyWsClient;
  private final ExecutorService executor;

  public AbcdToAvroCallback(XmlToAvroConfiguration config, MessagePublisher publisher, CuratorFramework curator,
      PipelinesHistoryWsClient historyWsClient, ExecutorService executor) {
    this.curator = checkNotNull(curator, "curator cannot be null");
    this.config = checkNotNull(config, "config cannot be null");
    this.executor = checkNotNull(executor, "executor cannot be null");
    this.publisher = publisher;
    this.historyWsClient = historyWsClient;
  }

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
        LOG.info("Skip the message, cause it wasn't modified, exit from handler");
        return;
      }

      LOG.info("Message handler began - {}", message);

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
          XmlToAvroCallback.createRunnable(config, datasetId, attempt.toString(), endpointType, executor, SKIP_RECORDS_CHECK);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      PipelineCallback.create()
          .incomingMessage(message)
          .outgoingMessage(new PipelinesVerbatimMessage(datasetId, attempt, config.interpretTypes, steps, endpointType))
          .curator(curator)
          .zkRootElementPath(STEP.getLabel())
          .pipelinesStepName(STEP)
          .publisher(publisher)
          .runnable(runnable)
          .historyWsClient(historyWsClient)
          .metricsSupplier(metricsSupplier(datasetId, attempt))
          .build()
          .handleMessage();

      LOG.info("Message handler ended - {}", message);
    }
  }

  private Supplier<List<PipelineStep.MetricInfo>> metricsSupplier(UUID datasetId, int attempt) {
    return () ->
        HdfsUtils.readMetricsFromMetaFile(
            config.hdfsSiteConfig,
            buildOutputPathAsString(
                config.repositoryPath,
                datasetId.toString(),
                String.valueOf(attempt),
                config.metaFileName));
  }
}
