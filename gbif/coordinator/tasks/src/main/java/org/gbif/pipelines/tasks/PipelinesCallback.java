package org.gbif.pipelines.tasks;

import static org.gbif.common.messaging.api.messages.OccurrenceDeletionReason.NOT_SEEN_IN_LAST_CRAWL;

import com.google.common.annotations.VisibleForTesting;
import io.github.resilience4j.retry.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.PipelineStep.MetricInfo;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.PipelinesWorkflow.Graph;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.pipelines.ws.PipelineProcessParameters;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage.DatasetInfo;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesRunnerMessage;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.registry.ws.client.DatasetClient;
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

  private static final Retry RUNNING_EXECUTION_CALL =
      Retry.of(
          "runningExecutionCall",
          RetryConfig.custom()
              .maxAttempts(3)
              .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(3)))
              .retryOnResult(Objects::isNull)
              .build());

  private static final Set<PipelineStep.Status> PROCESSED_STATE_SET =
      new HashSet<>(
          Arrays.asList(
              PipelineStep.Status.RUNNING,
              PipelineStep.Status.FAILED,
              PipelineStep.Status.COMPLETED,
              PipelineStep.Status.ABORTED));

  private static final Set<PipelineStep.Status> FINISHED_STATE_SET =
      new HashSet<>(Arrays.asList(PipelineStep.Status.COMPLETED, PipelineStep.Status.ABORTED));

  private static Properties properties;
  private final MessagePublisher publisher;
  @NonNull private final StepType stepType;
  @NonNull private final PipelinesHistoryClient historyClient;
  private final DatasetClient datasetClient;
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
    Optional<TrackingInfo> info = Optional.empty();

    try (MDCCloseable mdc = MDC.putCloseable("datasetKey", datasetKey);
        MDCCloseable mdc1 = MDC.putCloseable("attempt", message.getAttempt().toString());
        MDCCloseable mdc2 = MDC.putCloseable("step", stepType.name())) {

      if (!handler.isMessageCorrect(message) || isProcessingStopped() || isValidatorAborted()) {
        log.info(
            "Skip the message, please check that message is correct/runner/validation info/etc, exit from handler");
        return;
      }

      O outgoingMessage = handler.createOutgoingMessage(message);

      // track the pipeline step
      info = trackPipelineStep();

      log.info("Message handler began - {}", message);
      Runnable runnable = handler.createRunnable(message);

      log.info("Handler has been started, datasetKey - {}", datasetKey);
      checkIfDatasetIsDeleted();
      runnable.run();
      checkIfDatasetIsDeleted();
      log.info("Handler has been finished, datasetKey - {}", datasetKey);

      // update tracking status
      info.ifPresent(i -> updateTrackingStatus(i, PipelineStep.Status.COMPLETED));

      // Send a wrapped outgoing message to Balancer queue
      if (outgoingMessage != null) {

        // set the executionId
        info.ifPresent(i -> outgoingMessage.setExecutionId(i.executionId));

        String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
        String messagePayload = outgoingMessage.toString();
        publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));

        String logInfo =
            "Next message has been sent - "
                + outgoingMessage.getClass().getSimpleName()
                + ":"
                + outgoingMessage;
        log.info(logInfo);
        info.ifPresent(this::updateQueuedStatus);
      }

      updateValidatorInfoStatus(Status.FINISHED);

    } catch (Exception ex) {
      String error = "Error for datasetKey - " + datasetKey + " : " + ex.getMessage();
      log.error(error, ex);

      // update tracking status
      info.ifPresent(i -> updateTrackingStatus(i, PipelineStep.Status.FAILED));

      // update validator info
      updateValidatorInfoStatus(Status.FAILED);
    } finally {
      if (message.getExecutionId() != null) {
        log.info("Mark execution as FINISHED if all steps are FINISHED");
        historyClient.markPipelineExecutionIfFinished(message.getExecutionId());
      }
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

  private boolean isProcessingStopped() {
    if (isValidator) {
      return false;
    }

    Long currentKey = message.getExecutionId();

    Supplier<Long> s = () -> historyClient.getRunningExecutionKey(message.getDatasetUuid());
    Long runningKey;
    if (currentKey == null) {
      runningKey = s.get();
    } else {
      // if current key is not null, running key must not be null unless execution was aborted,
      // check multiple times
      runningKey = RUNNING_EXECUTION_CALL.executeSupplier(s);
    }
    if (currentKey == null && runningKey == null) {
      log.info("Continue execution. New execution and no other running executions");
      return false;
    }
    if (currentKey == null) {
      log.warn("Can't run new execution if some other execution is running");
      return true;
    }
    if (runningKey == null) {
      log.warn("Stop execution. Execution is aborted");
      return true;
    }
    // Stop the process if execution keys are different
    return !currentKey.equals(runningKey);
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
        DatasetInfo datasetInfo = message.getDatasetInfo();
        boolean containsOccurrences = datasetInfo.isContainsOccurrences();
        boolean containsEvents = false;
        if (datasetInfo.getDatasetType() == DatasetType.SAMPLING_EVENT) {
          containsEvents = datasetInfo.isContainsEvents();
        }

        Set<StepType> stepTypes =
            PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents)
                .getAllNodesFor(Collections.singleton(stepType));

        PipelineExecution execution =
            new PipelineExecution().setStepsToRun(stepTypes).setCreated(LocalDateTime.now());

        executionId = historyClient.addPipelineExecution(processKey, execution);
        message.setExecutionId(executionId);
      }

      List<PipelineStep> stepsByExecutionKey =
          historyClient.getPipelineStepsByExecutionKey(executionId);

      // add step to the process
      PipelineStep step =
          stepsByExecutionKey.stream()
              .filter(ps -> ps.getType() == stepType)
              .findAny()
              .orElseThrow(
                  () ->
                      new PipelinesException(
                          "History service doesn't contain stepType: " + stepType));

      if (PROCESSED_STATE_SET.contains(step.getState())) {
        log.error(
            "Dataset is in the queue, please check the pipeline-ingestion monitoring tool - {}",
            datasetUuid);
        throw new PipelinesException(
            "Dataset is in the queue, please check the pipeline-ingestion monitoring tool");
      }

      step.setMessage(OBJECT_MAPPER.writeValueAsString(message))
          .setState(PipelineStep.Status.RUNNING)
          .setRunner(StepRunner.valueOf(getRunner()))
          .setStarted(LocalDateTime.now())
          .setPipelinesVersion(getPipelinesVersion());

      long stepKey = historyClient.updatePipelineStep(step);

      Map<StepType, PipelineStep> pipelineStepMap =
          stepsByExecutionKey.stream()
              .collect(Collectors.toMap(PipelineStep::getType, Function.identity()));

      TrackingInfo trackingInfo =
          TrackingInfo.builder()
              .processKey(processKey)
              .executionId(executionId)
              .pipelineStepMap(pipelineStepMap)
              .stepKey(stepKey)
              .datasetId(datasetUuid.toString())
              .attempt(attempt.toString())
              .build();

      return Optional.of(trackingInfo);

    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error("Couldn't track pipeline step for message {}", message, ex);
      return Optional.empty();
    }
  }

  private void updateQueuedStatus(TrackingInfo info) {
    List<Graph<StepType>.Edge> nodeEdges;
    if (isValidator) {
      nodeEdges = PipelinesWorkflow.getValidatorWorkflow().getNodeEdges(stepType);
    } else {
      DatasetInfo datasetInfo = message.getDatasetInfo();
      boolean containsOccurrences = datasetInfo.isContainsOccurrences();
      boolean containsEvents = false;
      if (datasetInfo.getDatasetType() == DatasetType.SAMPLING_EVENT) {
        containsEvents = datasetInfo.isContainsEvents();
      }
      Graph<StepType> workflow = PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents);
      nodeEdges = workflow.getNodeEdges(stepType);
    }
    nodeEdges.forEach(
        e -> {
          PipelineStep step = info.pipelineStepMap.get(e.getNode());
          if (step != null) {
            step.setState(PipelineStep.Status.QUEUED);
            historyClient.updatePipelineStep(step);
          }
        });
  }

  private void updateTrackingStatus(TrackingInfo ti, PipelineStep.Status status) {
    String path =
        HdfsUtils.buildOutputPathAsString(
            config.getRepositoryPath(), ti.datasetId, ti.attempt, config.getMetaFileName());
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());
    List<MetricInfo> metricInfos = HdfsUtils.readMetricsFromMetaFile(hdfsConfigs, path);

    PipelineStep pipelineStep = historyClient.getPipelineStep(ti.stepKey);
    pipelineStep.setState(status);
    pipelineStep.setMetrics(new HashSet<>(metricInfos));

    if (metricInfos.size() == 1) {
      Optional.ofNullable(metricInfos.get(0).getValue())
          .filter(v -> !v.isEmpty())
          .map(Long::parseLong)
          .ifPresent(pipelineStep::setNumberRecords);
    } else if (metricInfos.size() > 1) {
      pipelineStep.setNumberRecords(-1L);
    }

    if (FINISHED_STATE_SET.contains(status)) {
      pipelineStep.setFinished(LocalDateTime.now());
    }

    try {
      Runnable r = () -> historyClient.updatePipelineStep(pipelineStep);
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

  private void checkIfDatasetIsDeleted() throws IOException {
    if (datasetClient != null) {
      Dataset dataset = datasetClient.get(message.getDatasetUuid());
      if (dataset != null && dataset.getDeleted() != null) {
        publisher.send(
            new DeleteDatasetOccurrencesMessage(message.getDatasetUuid(), NOT_SEEN_IN_LAST_CRAWL));
        log.error("The dataset marked as deleted while was being in the processing");
        throw new PipelinesException("The dataset marked as deleted");
      }
    }
  }

  @Builder
  private static class TrackingInfo {
    long processKey;
    long executionId;
    long stepKey;
    String datasetId;
    String attempt;
    Map<StepType, PipelineStep> pipelineStepMap;
  }
}
