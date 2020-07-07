package org.gbif.pipelines.crawler;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.PipelineStep.MetricInfo;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.pipelines.ws.PipelineStepParameters;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;
import org.gbif.utils.file.properties.PropertiesUtil;

import org.apache.curator.framework.CuratorFramework;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

/**
 * Common class for building and handling a pipeline step. Contains {@link Builder} to simplify the creation process
 * and main handling process. Please see the main method {@link PipelinesCallback#handleMessage}
 */
@Slf4j
@Builder
public class PipelinesCallback<I extends PipelineBasedMessage, O extends PipelineBasedMessage> {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Retry RETRY =
      Retry.of(
          "registryCall",
          RetryConfig.custom()
              .maxAttempts(3)
              .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(3)))
              .build()
      );

  private static Properties properties;
  private final MessagePublisher publisher;
  @NonNull
  private final StepType stepType;
  @NonNull
  private final CuratorFramework curator;
  @NonNull
  private final PipelinesHistoryWsClient client;
  @NonNull
  private final BaseConfiguration config;
  @NonNull
  private final I message;
  @NonNull
  private final StepHandler<I, O> handler;

  static {
    try {
      properties = PropertiesUtil.loadProperties("pipelines.properties");
    } catch (IOException e) {
      log.error("Couldn't load pipelines properties", e);
    }
  }

  /**
   * The main process handling:
   * <p>
   * 1) Receives a MQ message
   * 2) Updates Zookeeper start date monitoring metrics
   * 3) Create pipeline step in tracking service
   * 4) Runs runnable function, which is the main message processing logic
   * 5) Updates Zookeeper end date monitoring metrics
   * 6) Update status in tracking service
   * 7) Sends a wrapped message to Balancer microservice
   * 8) Updates Zookeeper successful or error monitoring metrics
   * 9) Cleans Zookeeper monitoring metrics if the received message is the last
   */
  public void handleMessage() {

    String datasetKey = message.getDatasetUuid().toString();
    O outgoingMessage = handler.createOutgoingMessage(message);
    Optional<TrackingInfo> trackingInfo = Optional.empty();

    try (MDCCloseable mdc = MDC.putCloseable("datasetKey", datasetKey);
        MDCCloseable mdc1 = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDCCloseable mdc2 = MDC.putCloseable("step", stepType.name())) {

      if (!handler.isMessageCorrect(message)) {
        log.info("Skip the message, cause the runner is different or it wasn't modified, exit from handler");
        return;
      }

      log.info("Message handler began - {}", message);
      Runnable runnable = handler.createRunnable(message);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ message
      // Short variables
      Set<String> steps = message.getPipelineSteps();

      // Check the step
      if (!steps.contains(stepType.name())) {
        return;
      }

      log.info("Message has been received {}", message);
      if (ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(datasetKey, stepType.getLabel()))) {
        log.error("Dataset is in the queue, please check the pipeline-ingestion monitoring tool - {}", datasetKey);
        return;
      }

      // track the pipeline step
      trackingInfo = trackPipelineStep();

      String crawlerZkPath = CrawlerNodePaths.getCrawlInfoPath(message.getDatasetUuid(), PROCESS_STATE_OCCURRENCE);
      if (ZookeeperUtils.checkExists(curator, crawlerZkPath)) {
        ZookeeperUtils.updateMonitoring(curator, crawlerZkPath, "RUNNING");
      }

      String mqMessageZkPath = Fn.MQ_MESSAGE.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(curator, datasetKey, mqMessageZkPath, message.toString());

      String mqClassNameZkPath = Fn.MQ_CLASS_NAME.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(curator, datasetKey, mqClassNameZkPath, message.getClass().getCanonicalName());

      String startDateZkPath = Fn.START_DATE.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoringDate(curator, datasetKey, startDateZkPath);

      String runnerZkPath = Fn.RUNNER.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(curator, datasetKey, runnerZkPath, getRunner());

      log.info("Handler has been started, datasetKey - {}", datasetKey);
      runnable.run();
      log.info("Handler has been finished, datasetKey - {}", datasetKey);

      String endDatePath = Fn.END_DATE.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoringDate(curator, datasetKey, endDatePath);

      // update tracking status
      trackingInfo.ifPresent(info -> updateTrackingStatus(info, PipelineStep.Status.COMPLETED));

      // Send a wrapped outgoing message to Balancer queue
      if (outgoingMessage != null) {
        String successfulPath = Fn.SUCCESSFUL_AVAILABILITY.apply(stepType.getLabel());
        ZookeeperUtils.updateMonitoring(curator, datasetKey, successfulPath, Boolean.TRUE.toString());

        // set the executionId
        trackingInfo.ifPresent(info -> outgoingMessage.setExecutionId(info.executionId));

        String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
        String messagePayload = outgoingMessage.toString();
        publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));

        String info = "Next message has been sent - " + outgoingMessage;
        log.info(info);

        String successfulMessagePath = Fn.SUCCESSFUL_MESSAGE.apply(stepType.getLabel());
        ZookeeperUtils.updateMonitoring(curator, datasetKey, successfulMessagePath, info);
      }

      // Change zookeeper counter for passed steps
      ZookeeperUtils.checkMonitoringById(curator, steps.size(), datasetKey);

    } catch (Exception ex) {
      String error = "Error for datasetKey - " + datasetKey + " : " + ex.getMessage();
      log.error(error, ex);

      String errorPath = Fn.ERROR_AVAILABILITY.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(curator, datasetKey, errorPath, Boolean.TRUE.toString());

      String errorMessagePath = Fn.ERROR_MESSAGE.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(curator, datasetKey, errorMessagePath, error);

      // update tracking status
      trackingInfo.ifPresent(info -> updateTrackingStatus(info, PipelineStep.Status.FAILED));
    }

    log.info("Message handler ended - {}", message);

  }

  private Optional<TrackingInfo> trackPipelineStep() {
    try {
      // create pipeline process. If it already exists it returns the existing one (the db query does an upsert).
      UUID datasetUuid = message.getDatasetUuid();
      Integer attempt = message.getAttempt();
      long processKey = client.createOrGetPipelineProcess(datasetUuid, attempt);

      Long executionId = message.getExecutionId();
      if (executionId == null) {
        // create execution
        PipelineExecution execution = new PipelineExecution().setStepsToRun(Collections.singletonList(stepType));

        executionId = client.addPipelineExecution(processKey, execution);
        message.setExecutionId(executionId);
      }

      // add step to the process
      PipelineStep step =
          new PipelineStep()
              .setMessage(OBJECT_MAPPER.writeValueAsString(message))
              .setType(stepType)
              .setState(PipelineStep.Status.RUNNING)
              .setRunner(StepRunner.valueOf(getRunner()))
              .setPipelinesVersion(getPipelinesVersion());

      long stepKey = client.addPipelineStep(processKey, executionId, step);

      return Optional.of(
          new TrackingInfo(processKey, executionId, stepKey, datasetUuid.toString(), attempt.toString()));
    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error("Couldn't track pipeline step for message {}", message, ex);
      return Optional.empty();
    }
  }

  private void updateTrackingStatus(TrackingInfo ti, PipelineStep.Status status) {
    String path =
        HdfsUtils.buildOutputPathAsString(config.getRepositoryPath(), ti.datasetId, ti.attempt, config.getMetaFileName());
    List<MetricInfo> metricInfos =
        HdfsUtils.readMetricsFromMetaFile(config.getHdfsSiteConfig(), config.getCoreSiteConfig(), path);
    PipelineStepParameters psp = new PipelineStepParameters(status, metricInfos);
    try {
      Runnable r = () -> client.updatePipelineStepStatusAndMetrics(ti.processKey, ti.executionId, ti.stepKey, psp);
      Retry.decorateRunnable(RETRY, r).run();
    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error("Couldn't update tracking status for process {} and step {}", ti.processKey, ti.stepKey, ex);
    }
  }

  @VisibleForTesting
  static String getPipelinesVersion() {
    return properties == null ? null : properties.getProperty("pipelines.version");
  }

  private String getRunner() {
    if (message instanceof PipelinesAbcdMessage
        || message instanceof PipelinesXmlMessage
        || message instanceof PipelinesDwcaMessage) {
      return StepRunner.STANDALONE.name();
    }
    if (message instanceof PipelinesIndexedMessage) {
      return ((PipelinesIndexedMessage) message).getRunner();
    }
    if (message instanceof PipelinesInterpretedMessage) {
      return ((PipelinesInterpretedMessage) message).getRunner();
    }
    if (message instanceof PipelinesVerbatimMessage) {
      return ((PipelinesVerbatimMessage) message).getRunner();
    }
    return StepRunner.UNKNOWN.name();
  }

  @AllArgsConstructor
  private static class TrackingInfo {

    long processKey;
    long executionId;
    long stepKey;
    String datasetId;
    String attempt;

  }

}
