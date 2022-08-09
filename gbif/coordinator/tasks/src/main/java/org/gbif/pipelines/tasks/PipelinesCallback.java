package org.gbif.pipelines.tasks;

import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.PipelineStep.MetricInfo;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.pipelines.ws.PipelineProcessParameters;
import org.gbif.api.model.pipelines.ws.PipelineStepParameters;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesRunnerMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.MDC;
import org.slf4j.MDC.MDCCloseable;

/**
 * Common class for building and handling a pipeline step. Contains {@link Builder} to simplify the
 * creation process and main handling process. Please see the main method {@link
 * PipelinesCallback#handleMessage}
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
              .build());

  private static Properties properties;
  private final MessagePublisher publisher;
  @NonNull private final StepType stepType;
  @NonNull private final CuratorFramework curator;
  @NonNull private final PipelinesHistoryClient historyClient;
  @NonNull private final BaseConfiguration config;
  @NonNull private final I message;
  @NonNull private final StepHandler<I, O> handler;
  private final ValidationWsClient validationClient;
  @Builder.Default private final boolean isValidator = false;

  static {
    try {
      properties = PropertiesUtil.loadProperties("pipelines.properties");
    } catch (IOException e) {
      log.error("Couldn't load pipelines properties", e);
    }
  }

  /**
   * The main process handling:
   *
   * <pre>
   *   1) Receives a MQ message
   *   2) Updates Zookeeper start date monitoring metrics
   *   3) Create pipeline step in tracking service
   *   4) Runs runnable function, which is the main message processing logic
   *   5) Updates Zookeeper end date monitoring metrics
   *   6) Update status in tracking service
   *   7) Sends a wrapped message to Balancer microservice
   *   8) Updates Zookeeper successful or error monitoring metrics
   *   9) Cleans Zookeeper monitoring metrics if the received message is the
   * last
   * </pre>
   */
  public void handleMessage() {

    String datasetKey = message.getDatasetUuid().toString();
    Optional<TrackingInfo> trackingInfo = Optional.empty();

    try (MDCCloseable mdc = MDC.putCloseable("datasetKey", datasetKey);
        MDCCloseable mdc1 = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDCCloseable mdc2 = MDC.putCloseable("step", stepType.name())) {

      O outgoingMessage = handler.createOutgoingMessage(message);

      if (!handler.isMessageCorrect(message) || isValidatorAborted()) {
        deleteValidatorZkPath(datasetKey);
        log.info(
            "Skip the message, please check that message is correct/runner/validation info/etc, exit from handler");
        return;
      }

      log.info("Message handler began - {}", message);
      Runnable runnable = handler.createRunnable(message);

      // Message callback handler, updates zookeeper info, runs process logic and sends next MQ
      // message
      // Short variables
      Set<String> steps = message.getPipelineSteps();

      // Check the step
      if (!steps.contains(stepType.name())) {
        log.info("Type is not specified in the pipelineSteps array");
        return;
      }

      // Check start date If CLI was restarted it will be empty
      String startDateZkPath = Fn.START_DATE.apply(stepType.getLabel());
      String fullPath = getPipelinesInfoPath(datasetKey, startDateZkPath, isValidator);

      log.info("Message has been received {}", message);
      if (ZookeeperUtils.getAsString(curator, fullPath).isPresent()) {
        log.error(
            "Dataset is in the queue, please check the pipeline-ingestion monitoring tool - {}",
            datasetKey);
        return;
      }

      // track the pipeline step
      trackingInfo = trackPipelineStep();

      String crawlerZkPath =
          CrawlerNodePaths.getCrawlInfoPath(message.getDatasetUuid(), PROCESS_STATE_OCCURRENCE);
      if (ZookeeperUtils.checkExists(curator, crawlerZkPath)) {
        ZookeeperUtils.updateMonitoring(curator, crawlerZkPath, "RUNNING");
      }

      String mqMessageZkPath = Fn.MQ_MESSAGE.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(
          curator, datasetKey, mqMessageZkPath, message.toString(), isValidator);

      String mqClassNameZkPath = Fn.MQ_CLASS_NAME.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(
          curator,
          datasetKey,
          mqClassNameZkPath,
          message.getClass().getCanonicalName(),
          isValidator);

      ZookeeperUtils.updateMonitoringDate(curator, datasetKey, startDateZkPath, isValidator);

      String runnerZkPath = Fn.RUNNER.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(curator, datasetKey, runnerZkPath, getRunner(), isValidator);
      updateValidatorInfoStatus(Status.RUNNING);

      log.info("Handler has been started, datasetKey - {}", datasetKey);
      runnable.run();
      log.info("Handler has been finished, datasetKey - {}", datasetKey);

      String endDatePath = Fn.END_DATE.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoringDate(curator, datasetKey, endDatePath, isValidator);

      // update tracking status
      trackingInfo.ifPresent(info -> updateTrackingStatus(info, PipelineStep.Status.COMPLETED));

      String successfulPath = Fn.SUCCESSFUL_AVAILABILITY.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(
          curator, datasetKey, successfulPath, Boolean.TRUE.toString(), isValidator);

      // Send a wrapped outgoing message to Balancer queue
      if (outgoingMessage != null) {

        // set the executionId
        trackingInfo.ifPresent(info -> outgoingMessage.setExecutionId(info.executionId));

        String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
        String messagePayload = outgoingMessage.toString();
        publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));

        String info =
            "Next message has been sent - "
                + outgoingMessage.getClass().getSimpleName()
                + ":"
                + outgoingMessage;
        log.info(info);

        String successfulMessagePath = Fn.SUCCESSFUL_MESSAGE.apply(stepType.getLabel());
        ZookeeperUtils.updateMonitoring(
            curator, datasetKey, successfulMessagePath, info, isValidator);
      }

      // Change zookeeper counter for passed steps
      ZookeeperUtils.checkMonitoringById(curator, steps.size(), datasetKey, isValidator);

      updateValidatorInfoStatus(Status.FINISHED);

    } catch (Exception ex) {
      String error = "Error for datasetKey - " + datasetKey + " : " + ex.getMessage();
      log.error(error, ex);

      String errorPath = Fn.ERROR_AVAILABILITY.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(
          curator, datasetKey, errorPath, Boolean.TRUE.toString(), isValidator);

      String errorMessagePath = Fn.ERROR_MESSAGE.apply(stepType.getLabel());
      ZookeeperUtils.updateMonitoring(curator, datasetKey, errorMessagePath, error, isValidator);

      // update tracking status
      trackingInfo.ifPresent(info -> updateTrackingStatus(info, PipelineStep.Status.FAILED));

      // update validator info
      updateValidatorInfoStatus(Status.FAILED);
      deleteValidatorZkPath(datasetKey);

      // Mark crawler as finished
      ZookeeperUtils.markCrawlerAsFinished(curator, datasetKey);
    }

    log.info("Message handler ended - {}", message);
  }

  private boolean isValidatorAborted() {
    if (isValidator) {
      Validation validation = validationClient.get(message.getDatasetUuid());
      if (validation != null) {
        Status status = validation.getStatus();
        return status == Status.ABORTED
            || status == Status.FINISHED
            || status == Status.FAILED
            || status == Status.DOWNLOADING;
      } else {
        log.warn(
            "Can't find validation data key {}, please check that record exists",
            message.getDatasetUuid());
      }
    }
    return false;
  }

  @SneakyThrows
  private void deleteValidatorZkPath(String datasetKey) {
    if (isValidator) {
      String path = getPipelinesInfoPath(datasetKey, isValidator);
      curator.delete().deletingChildrenIfNeeded().forPath(path);
    }
  }

  private void updateValidatorInfoStatus(Status status) {
    if (isValidator) {
      Validations.updateStatus(validationClient, message.getDatasetUuid(), stepType, status);
    }
  }

  private Optional<TrackingInfo> trackPipelineStep() {
    try {

      if (isValidator) {
        return Optional.empty();
      }

      // create pipeline process. If it already exists it returns the existing one (the db query
      // does an upsert).
      UUID datasetUuid = message.getDatasetUuid();
      Integer attempt = message.getAttempt();
      long processKey =
          historyClient.createPipelineProcess(new PipelineProcessParameters(datasetUuid, attempt));

      Long executionId = message.getExecutionId();
      if (executionId == null) {
        // create execution
        PipelineExecution execution =
            new PipelineExecution().setStepsToRun(Collections.singletonList(stepType));

        executionId = historyClient.addPipelineExecution(processKey, execution);
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

      long stepKey = historyClient.addPipelineStep(processKey, executionId, step);

      return Optional.of(
          new TrackingInfo(
              processKey, executionId, stepKey, datasetUuid.toString(), attempt.toString()));
    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error("Couldn't track pipeline step for message {}", message, ex);
      return Optional.empty();
    }
  }

  private void updateTrackingStatus(TrackingInfo ti, PipelineStep.Status status) {
    String path =
        HdfsUtils.buildOutputPathAsString(
            config.getRepositoryPath(), ti.datasetId, ti.attempt, config.getMetaFileName());
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());
    List<MetricInfo> metricInfos = HdfsUtils.readMetricsFromMetaFile(hdfsConfigs, path);
    PipelineStepParameters psp = new PipelineStepParameters(status, metricInfos);
    try {
      Runnable r =
          () ->
              historyClient.updatePipelineStepStatusAndMetrics(
                  ti.processKey, ti.executionId, ti.stepKey, psp);
      Retry.decorateRunnable(RETRY, r).run();
    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error(
          "Couldn't update tracking status for process {} and step {}",
          ti.processKey,
          ti.stepKey,
          ex);
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
    if (message instanceof PipelinesRunnerMessage) {
      return ((PipelinesRunnerMessage) message).getRunner();
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
