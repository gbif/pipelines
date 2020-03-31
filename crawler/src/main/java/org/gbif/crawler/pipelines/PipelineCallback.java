package org.gbif.crawler.pipelines;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineStep;
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
import org.gbif.crawler.common.utils.ZookeeperUtils;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
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
import lombok.extern.slf4j.Slf4j;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

/**
 * Common class for building and handling a pipeline step. Contains {@link Builder} to simplify the creation process
 * and main handling process. Please see the main method {@link PipelineCallback#handleMessage}
 */
@Slf4j
@Builder
public class PipelineCallback {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static Properties properties;
  private final Retry retry = createRetry();

  static {
    try {
      properties = PropertiesUtil.loadProperties("pipelines.properties");
    } catch (IOException e) {
      log.error("Couldn't load pipelines properties", e);
    }
  }

  private MessagePublisher publisher;
  private CuratorFramework curator;
  private PipelineBasedMessage incomingMessage;
  private PipelineBasedMessage outgoingMessage;
  private StepType pipelinesStepName;
  private String zkRootElementPath;
  private Runnable runnable;
  private PipelinesHistoryWsClient historyWsClient;
  private Supplier<List<PipelineStep.MetricInfo>> metricsSupplier;

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

    // Short variables
    PipelineBasedMessage inMessage = incomingMessage;
    Set<String> steps = inMessage.getPipelineSteps();

    // Check the step
    if (!steps.contains(pipelinesStepName.name())) {
      return;
    }

    // Start main process
    String crawlId = inMessage.getDatasetUuid().toString() + "_" + inMessage.getAttempt();
    Optional<TrackingInfo> trackingInfo = Optional.empty();

    try (MDCCloseable mdc = MDC.putCloseable("crawlId", crawlId)) {

      log.info("Message has been received {}", inMessage);
      if (ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, zkRootElementPath))) {
        log.warn("Dataset is already in pipelines queue, please check the pipeline-ingestion monitoring tool - {}",
            crawlId);
        return;
      }

      // track the pipeline step
      trackingInfo = trackPipelineStep();

      String mqMessagePath = Fn.MQ_MESSAGE.apply(zkRootElementPath);
      ZookeeperUtils.updateMonitoring(curator, crawlId, mqMessagePath, inMessage.toString());

      String mqClassNamePath = Fn.MQ_CLASS_NAME.apply(zkRootElementPath);
      ZookeeperUtils.updateMonitoring(curator, crawlId, mqClassNamePath, inMessage.getClass().getCanonicalName());

      String startDatePath = Fn.START_DATE.apply(zkRootElementPath);
      ZookeeperUtils.updateMonitoringDate(curator, crawlId, startDatePath);

      String runnerPath = Fn.RUNNER.apply(zkRootElementPath);
      ZookeeperUtils.updateMonitoring(curator, crawlId, runnerPath, getRunner(inMessage));

      log.info("Handler has been started, crawlId - {}", crawlId);
      runnable.run();
      log.info("Handler has been finished, crawlId - {}", crawlId);

      String endDatePath = Fn.END_DATE.apply(zkRootElementPath);
      ZookeeperUtils.updateMonitoringDate(curator, crawlId, endDatePath);

      // update tracking status
      trackingInfo.ifPresent(info -> updateTrackingStatus(info, PipelineStep.Status.COMPLETED));

      // Send a wrapped outgoing message to Balancer queue
      if (outgoingMessage != null) {
        String successfulPath = Fn.SUCCESSFUL_AVAILABILITY.apply(zkRootElementPath);
        ZookeeperUtils.updateMonitoring(curator, crawlId, successfulPath, Boolean.TRUE.toString());

        // set the executionId
        trackingInfo.ifPresent(info -> outgoingMessage.setExecutionId(info.executionId));

        String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
        String messagePayload = outgoingMessage.toString();
        publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));

        String info = "Next message has been sent - " + outgoingMessage;
        log.info(info);

        String successfulMessagePath = Fn.SUCCESSFUL_MESSAGE.apply(zkRootElementPath);
        ZookeeperUtils.updateMonitoring(curator, crawlId, successfulMessagePath, info);
      }

      // Change zookeeper counter for passed steps
      ZookeeperUtils.checkMonitoringById(curator, steps.size(), crawlId);

    } catch (Exception ex) {
      String error = "Error for crawlId - " + crawlId + " : " + ex.getMessage();
      log.error(error, ex);

      String errorPath = Fn.ERROR_AVAILABILITY.apply(zkRootElementPath);
      ZookeeperUtils.updateMonitoring(curator, crawlId, errorPath, Boolean.TRUE.toString());

      String errorMessagePath = Fn.ERROR_MESSAGE.apply(zkRootElementPath);
      ZookeeperUtils.updateMonitoring(curator, crawlId, errorMessagePath, error);

      // update tracking status
      trackingInfo.ifPresent(info -> updateTrackingStatus(info, PipelineStep.Status.FAILED));
    }
  }

  private Optional<TrackingInfo> trackPipelineStep() {
    try {
      // create pipeline process. If it already exists it returns the existing one (the db query does an upsert).
      long processKey =
          historyWsClient.createOrGetPipelineProcess(incomingMessage.getDatasetUuid(), incomingMessage.getAttempt());

      Long executionId = incomingMessage.getExecutionId();
      if (executionId == null) {
        // create execution
        PipelineExecution execution =
            new PipelineExecution().setStepsToRun(Collections.singletonList(pipelinesStepName));

        executionId = historyWsClient.addPipelineExecution(processKey, execution);
        incomingMessage.setExecutionId(executionId);
      }

      // add step to the process
      PipelineStep step =
          new PipelineStep()
              .setMessage(OBJECT_MAPPER.writeValueAsString(incomingMessage))
              .setType(pipelinesStepName)
              .setState(PipelineStep.Status.RUNNING)
              .setRunner(StepRunner.valueOf(getRunner(incomingMessage)))
              .setPipelinesVersion(getPipelinesVersion());

      long stepKey = historyWsClient.addPipelineStep(processKey, executionId, step);

      return Optional.of(new TrackingInfo(processKey, executionId, stepKey));
    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error("Couldn't track pipeline step for message {}", incomingMessage, ex);
      return Optional.empty();
    }
  }

  private void updateTrackingStatus(TrackingInfo trackingInfo, PipelineStep.Status status) {
    long processKey = trackingInfo.processKey;
    long stepKey = trackingInfo.stepKey;
    long executionId = trackingInfo.executionId;
    PipelineStepParameters psp = new PipelineStepParameters(status, metricsSupplier.get());
    try {
      Runnable r = () -> historyWsClient.updatePipelineStepStatusAndMetrics(processKey, executionId, stepKey, psp);
      Retry.decorateRunnable(retry, r).run();
    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error("Couldn't update tracking status for process {} and step {}", processKey, stepKey, ex);
    }
  }

  @VisibleForTesting
  static String getPipelinesVersion() {
    return properties == null ? null : properties.getProperty("pipelines.version");
  }

  private String getRunner(PipelineBasedMessage inMessage) {
    if (inMessage instanceof PipelinesAbcdMessage
        || inMessage instanceof PipelinesXmlMessage
        || inMessage instanceof PipelinesDwcaMessage) {
      return StepRunner.STANDALONE.name();
    }
    if (inMessage instanceof PipelinesIndexedMessage) {
      return ((PipelinesIndexedMessage) inMessage).getRunner();
    }
    if (inMessage instanceof PipelinesInterpretedMessage) {
      return ((PipelinesInterpretedMessage) inMessage).getRunner();
    }
    if (inMessage instanceof PipelinesVerbatimMessage) {
      return ((PipelinesVerbatimMessage) inMessage).getRunner();
    }
    return StepRunner.UNKNOWN.name();
  }

  private Retry createRetry() {
    RetryConfig retryConfig =
        RetryConfig.custom()
            .maxAttempts(3)
            .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(1)))
            .build();

    return Retry.of("registryCall", retryConfig);
  }

  @AllArgsConstructor
  private static class TrackingInfo {

    long processKey;
    long executionId;
    long stepKey;
  }
}
