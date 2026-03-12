package org.gbif.pipelines.coordinator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.gbif.api.model.pipelines.PipelineStep.Status.RUNNING;
import static org.gbif.pipelines.coordinator.PrometheusMetrics.*;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.base.Strings;
import feign.Contract;
import feign.Feign;
import feign.auth.BasicAuthRequestInterceptor;
import feign.httpclient.ApacheHttpClient;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.map.ObjectMapper;
import org.gbif.api.model.pipelines.*;
import org.gbif.api.model.pipelines.ws.PipelineProcessParameters;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public abstract class PipelinesCallback<
        I extends PipelineBasedMessage, O extends PipelineBasedMessage>
    implements MessageCallback<I>, AutoCloseable {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final String PAUSE_FILE_PATH = "/tmp/pause_message_processing";
  protected final PipelinesConfig pipelinesConfig;
  protected final PipelinesHistoryClient historyClient;
  protected final MessagePublisher publisher;
  protected final CloseableHttpClient httpClient;
  protected SparkSession sparkSession;
  protected FileSystem fileSystem;
  protected String sparkMaster;

  private static final Set<PipelineStep.Status> FINISHED_STATE_SET =
      new HashSet<>(
          Arrays.asList(
              PipelineStep.Status.COMPLETED,
              PipelineStep.Status.ABORTED,
              PipelineStep.Status.FAILED));

  private static final Set<PipelineStep.Status> PROCESSED_STATE_SET =
      new HashSet<>(
          Arrays.asList(
              RUNNING,
              PipelineStep.Status.FAILED,
              PipelineStep.Status.COMPLETED,
              PipelineStep.Status.ABORTED));

  private static final Retry RETRY =
      Retry.of(
          "registryCall",
          RetryConfig.custom()
              .maxAttempts(15)
              .retryExceptions(JsonParseException.class, IOException.class, TimeoutException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(30)))
              .build());

  private static final Retry RUNNING_EXECUTION_CALL =
      Retry.of(
          "runningExecutionCall",
          RetryConfig.custom()
              .maxAttempts(15)
              .retryExceptions(JsonParseException.class, IOException.class, TimeoutException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(30)))
              .retryOnResult(Objects::isNull)
              .build());

  public PipelinesCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String sparkMaster) {

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();

    this.pipelinesConfig = pipelinesConfig;
    this.publisher = publisher;
    this.historyClient =
        Feign.builder()
            .client(new ApacheHttpClient())
            .decoder(new JacksonDecoder(mapper))
            .encoder(new JacksonEncoder(mapper))
            .contract(new Contract.Default())
            .requestInterceptor(
                new BasicAuthRequestInterceptor(
                    pipelinesConfig.getStandalone().getRegistry().getUser(),
                    pipelinesConfig.getStandalone().getRegistry().getPassword()))
            .decode404()
            .target(
                PipelinesHistoryClient.class,
                pipelinesConfig.getStandalone().getRegistry().getWsUrl());
    this.httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();
    this.sparkMaster = sparkMaster;
  }

  public PipelinesCallback(PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    this(pipelinesConfig, publisher, null);
  }

  public void init() throws IOException {

    Configuration hadoopConf = null;
    if (isStandalone()) {
      SparkSession.Builder sparkBuilder = SparkSession.builder().appName("pipelines_standalone");
      sparkBuilder = sparkBuilder.master(sparkMaster);

      sparkBuilder.config("spark.driver.extraClassPath", "/etc/hadoop/conf");
      sparkBuilder.config("spark.executor.extraClassPath", "/etc/hadoop/conf");

      // let the individual implementations add their wares
      configSparkSession(sparkBuilder, pipelinesConfig);

      this.sparkSession = sparkBuilder.getOrCreate();

      hadoopConf = this.sparkSession.sparkContext().hadoopConfiguration();
    } else {
      hadoopConf = new Configuration();
    }

    if (pipelinesConfig.getHdfsSiteConfig() != null
        && pipelinesConfig.getCoreSiteConfig() != null) {
      hadoopConf.addResource(new Path(pipelinesConfig.getHdfsSiteConfig()));
      hadoopConf.addResource(new Path(pipelinesConfig.getCoreSiteConfig()));
      fileSystem = FileSystem.get(hadoopConf);
    } else {
      log.warn("Using local filesystem - this is suitable for local development only");
      fileSystem = FileSystem.getLocal(hadoopConf);
    }
  }

  public void close() throws IOException {
    if (sparkSession != null) {
      sparkSession.close();
    }
    if (fileSystem != null) {
      fileSystem.close();
    }
  }

  protected boolean isStandalone() {
    return true;
  }

  protected abstract StepType getStepType();

  protected boolean isMessageCorrect(I message) {
    if (!message.getPipelineSteps().contains(getStepType().name())) {
      log.error("The message doesn't contain {} type", getStepType().name());
      return false;
    }
    return true;
  }

  protected boolean pathExists(PipelinesEventsMessage message, String subdir) throws IOException {
    String inputPath =
        String.format(
            "%s/%s/%d/%s",
            pipelinesConfig.getOutputPath(),
            message.getDatasetUuid().toString(),
            message.getAttempt(),
            subdir);
    return fileSystem.exists(new Path(inputPath));
  }

  protected abstract void runPipeline(I message) throws Exception;

  protected abstract String getMetaFileName();

  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {}

  public void handleMessage(I message) {

    checkIfPaused();

    LAST_CONSUMED_MESSAGE_FROM_QUEUE_MS.set(System.currentTimeMillis());
    MESSAGES_READ_FROM_QUEUE.inc();

    ThreadContext.put(
        "datasetKey",
        message.getDatasetUuid() != null ? message.getDatasetUuid().toString() : "NO_DATASET");
    log.debug("Received message: {}", message);

    if (!isMessageCorrect(message) || isProcessingStopped(message)) {

      log.debug(
          "Returning message correct: {} isProcessingStopped: {}",
          isMessageCorrect(message),
          isProcessingStopped(message));
      return;
    }

    TrackingInfo trackingInfo = null;
    ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
    ThreadContext.put("attempt", message.getAttempt().toString());
    ThreadContext.put("step", getStepType().name());

    try {
      log.info(
          "Processing executionId {}, step {} attempt {}",
          message.getExecutionId(),
          message.getAttempt(),
          getStepType().name());

      trackingInfo = trackPipelineStep(message);

      CONCURRENT_DATASETS.inc();

      // Run pipeline for this callback
      runPipeline(message);

      COMPLETED_DATASETS.inc();

      // Acknowledge message processing
      updateTrackingStatus(trackingInfo, message, PipelineStep.Status.COMPLETED);

      // set outgoing message to the queue for the next step
      sendOutgoingMessage(trackingInfo, message);

      log.info("Finished processing datasetKey: {}", message.getDatasetUuid());

    } catch (Exception ex) {

      try {

        DATASETS_ERRORED_COUNT.inc();
        LAST_DATASETS_ERROR.set(System.currentTimeMillis());

        // FIXMETrackingInfo trackingInfo = trackPipelineStep(message);
        ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
        String error =
            "Error for datasetKey - " + message.getDatasetUuid() + " : " + ex.getMessage();
        log.error(error, ex);

        // update tracking status
        if (trackingInfo != null) {
          updateTrackingStatus(trackingInfo, message, PipelineStep.Status.FAILED);
        }

      } catch (Exception e) {
        ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
        log.error(
            "Failed to update tracking status for datasetKey - " + message.getDatasetUuid(), e);
      }
      //
      //                // update validator info
      //                String errorMessage = null;
      //                if (ex.getCause() instanceof PipelinesException) {
      //                    errorMessage = ((PipelinesException) ex.getCause()).getShortMessage();
      //                }
      //                updateValidatorInfoStatus(Status.FAILED, errorMessage);
    } finally {

      CONCURRENT_DATASETS.dec();

      if (message.getExecutionId() != null) {
        ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
        log.debug("Mark execution as FINISHED if all steps are FINISHED");
        Runnable r =
            () -> {
              log.debug(
                  "History client: mark pipeline execution if finished, executionId {}",
                  message.getExecutionId());
              historyClient.markPipelineExecutionIfFinished(message.getExecutionId());
            };
        Retry.decorateRunnable(RETRY, r).run();
      } else {
        log.warn(
            "Execution id is null for datasetKey {}, can't mark execution as FINISHED if all steps are FINISHED",
            message.getDatasetUuid());
      }
    }
  }

  private void sendOutgoingMessage(TrackingInfo trackingInfo, I message) throws IOException {
    Function<Long, List<PipelineStep>> getStepsByExecutionKeyFn =
        ek -> {
          log.debug("History client: get steps by execution key {}", ek);
          return historyClient.getPipelineStepsByExecutionKey(ek);
        };

    List<PipelineStep> executionPipelineSteps =
        Retry.decorateFunction(RETRY, getStepsByExecutionKeyFn).apply(trackingInfo.executionId);

    log.info(
        "Execution steps for execution key {}: {}",
        trackingInfo.executionId,
        executionPipelineSteps);

    log.debug(
        "Execution ID {}, steps size: {}, steps: {}",
        trackingInfo.executionId,
        executionPipelineSteps.size(),
        executionPipelineSteps.stream()
            .map(ps -> ps.getType().name())
            .collect(Collectors.joining(", ")));

    List<PipelineStep> thisPipelineStep =
        executionPipelineSteps.stream().filter(ps -> ps.getType() == getStepType()).toList();

    if (thisPipelineStep.isEmpty()) {
      // expected when we opt to only execute one step with &onlyRequestedStep=true
      log.warn(
          "Execution ID {}, current step {} is not found in the execution steps, won't send outgoing message. Available steps: {}",
          trackingInfo.executionId,
          getStepType(),
          executionPipelineSteps.stream()
              .map(ps -> ps.getType().name())
              .collect(Collectors.joining(", ")));
      return;
    }

    // if there is no more steps in the execution to run, dont send messages
    if (!executionPipelineSteps.isEmpty()) {
      // are there any incomplete steps left in the execution? if not,
      // don't send message to balancer, just mark execution as finished
      Set<PipelineStep> unprocessed =
          executionPipelineSteps.stream()
              .filter(ps -> !PROCESSED_STATE_SET.contains(ps.getState()))
              .collect(Collectors.toSet());
      if (unprocessed.isEmpty()) {
        log.info(
            "Execution ID {}, all steps are processed for execution, won't send outgoing message. Steps: {}",
            trackingInfo.executionId,
            executionPipelineSteps.stream()
                .map(ps -> ps.getType().name() + ":" + ps.getState().name())
                .collect(Collectors.joining(", ")));
        return;
      }
    }

    // Create and send outgoing message
    O outgoingMessage;
    try {
      outgoingMessage = createOutgoingMessage(message);
    } catch (Exception e) {
      log.error(
          "Failed to create outgoing message for executionID {} dataset {}: {}",
          trackingInfo.executionId,
          message.getDatasetUuid(),
          e.getMessage(),
          e);
      return;
    }

    if (outgoingMessage == null) {
      log.warn(
          "createOutgoingMessage returned null for dataset {} and executionID {}, won't send outgoing message",
          message.getDatasetUuid(),
          trackingInfo.executionId);
      return;
    }

    if (publisher == null) {
      log.error(
          "Message publisher is null, cannot send outgoing message for dataset {} and executionID {}",
          message.getDatasetUuid(),
          trackingInfo.executionId);
      return;
    }

    String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
    String messagePayload = outgoingMessage.toString();

    try {
      publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));
      log.info(
          "Message sent to balancer for {}, executionId: {}, step {}",
          outgoingMessage.getDatasetUuid(),
          outgoingMessage.getExecutionId(),
          this.getStepType().name());
    } catch (Exception e) {
      log.error(
          "Failed to send outgoing message for dataset {} and executionID {} after retries: {}",
          message.getDatasetUuid(),
          trackingInfo.executionId,
          e.getMessage(),
          e);
      return;
    }

    try {
      updateQueuedStatus(trackingInfo, message);
    } catch (Exception e) {
      log.error(
          "Failed to update queued status after sending outgoing message for dataset {} and executionID {}: {}",
          message.getDatasetUuid(),
          trackingInfo.executionId,
          e.getMessage(),
          e);
    }
  }

  private static void checkIfPaused() {
    while (new File(PAUSE_FILE_PATH).exists()) {
      log.warn(
          "Found "
              + PAUSE_FILE_PATH
              + " file, pausing processing new messages for 10s. Delete to resume.");
      try {
        Thread.sleep(30_000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private boolean isProcessingStopped(I message) {

    Long currentKey = message.getExecutionId();
    UUID datasetKey = message.getDatasetUuid();

    Supplier<Long> s = () -> historyClient.getRunningExecutionKey(datasetKey);
    Long runningKey;
    if (currentKey == null) {
      runningKey = s.get();
    } else {
      // if current key is not null, running key must not be null unless execution was aborted,
      // check multiple times
      runningKey = RUNNING_EXECUTION_CALL.executeSupplier(s);
    }
    if (currentKey == null && runningKey == null) {
      log.info(
          "Continue execution. New execution and no other running executions for {}", datasetKey);
      return false;
    }
    if (currentKey == null) {
      log.warn("Can't run new execution if some other execution is running for {}", datasetKey);
      return true;
    }
    if (runningKey == null) {
      log.warn("Stop execution. Execution is aborted for {}", datasetKey);
      return true;
    }
    // Stop the process if execution keys are different
    if (!currentKey.equals(runningKey)) {
      log.warn(
          "Stop the process if execution keys are different for {}, running key {}, currentKey {}",
          datasetKey,
          runningKey,
          currentKey);
      return true;
    }

    return false;
  }

  /**
   * Reads a yaml file and returns all the values
   *
   * @param filePath to a yaml file
   */
  public static List<PipelineStep.MetricInfo> readMetricsFromMetaFile(
      FileSystem fs, String filePath) {
    Path fsPath = new Path(filePath);
    try {
      if (fs.exists(fsPath)) {
        try (BufferedReader br =
            new BufferedReader(new InputStreamReader(fs.open(fsPath), UTF_8))) {
          return br.lines()
              .map(x -> x.replace("\u0000", ""))
              .filter(s -> !Strings.isNullOrEmpty(s))
              .map(z -> z.split(":"))
              .filter(s -> s.length > 1)
              .map(v -> new PipelineStep.MetricInfo(v[0].trim(), v[1].trim()))
              .collect(Collectors.toList());
        }
      }
    } catch (IOException e) {
      log.warn("Couldn't read meta file from {}", filePath, e);
    }
    return new ArrayList<>();
  }

  private void updateTrackingStatus(
      TrackingInfo trackingInfo, I message, PipelineStep.Status status) {

    String path =
        String.join(
            "/",
            pipelinesConfig.getOutputPath(),
            trackingInfo.datasetId,
            trackingInfo.attempt,
            getMetaFileName());

    List<PipelineStep.MetricInfo> metricInfos = readMetricsFromMetaFile(fileSystem, path);

    Function<Long, PipelineStep> getPipelineStepFn =
        sk -> {
          log.debug("History client: get steps by execution key {}", sk);
          return historyClient.getPipelineStep(sk);
        };
    PipelineStep pipelineStep =
        Retry.decorateFunction(RETRY, getPipelineStepFn).apply(trackingInfo.stepKey);

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
      pipelineStep.setFinished(OffsetDateTime.now());
      LAST_COMPLETED_MESSAGE_MS.set(System.currentTimeMillis());
    }

    try {
      Function<PipelineStep, Long> pipelineStepFn =
          s -> {
            log.debug("History client: update pipeline step: {}", s);
            PipelineStep step = historyClient.getPipelineStep(s.getKey());
            if (FINISHED_STATE_SET.contains(step.getState())) {
              return step.getKey();
            }
            return historyClient.updatePipelineStep(s.getKey(), s);
          };
      long stepKey = Retry.decorateFunction(RETRY, pipelineStepFn).apply(pipelineStep);
      log.debug(
          "Step key {}, step type {} is {}",
          stepKey,
          pipelineStep.getType(),
          pipelineStep.getState());

    } catch (Exception ex) {
      // we don't want to break the crawling if the tracking fails
      log.error("Couldn't update tracking status for dataset {}", message.getDatasetUuid(), ex);
    }
  }

  public abstract O createOutgoingMessage(I message);

  private void updateQueuedStatus(TrackingInfo info, I message) {

    List<PipelinesWorkflow.Graph<StepType>.Edge> nodeEdges;
    if (false /* isValidator*/) {
      nodeEdges = PipelinesWorkflow.getValidatorWorkflow().getNodeEdges(getStepType());
    } else {
      boolean containsEvents = containsEvents(message);
      boolean containsOccurrences = message.getDatasetInfo().isContainsOccurrences();
      PipelinesWorkflow.Graph<StepType> workflow =
          PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents);
      nodeEdges = workflow.getNodeEdges(getStepType());

      if (log.isDebugEnabled() && nodeEdges != null) {
        log.debug(
            "Workflow for {} {} containsOccurrences: {}, containsEvents: {} has nodes {} ",
            message.getDatasetInfo().getDatasetType(),
            message.getDatasetUuid(),
            containsOccurrences,
            containsEvents,
            nodeEdges.stream().map(e -> e.getNode().name()).collect(Collectors.joining(", ")));
      }
    }

    if (nodeEdges == null || nodeEdges.isEmpty()) {
      log.debug("No next steps found for step type {}", getStepType());
      return;
    }

    for (PipelinesWorkflow.Graph<StepType>.Edge e : nodeEdges) {
      PipelineStep step = info.pipelineStepMap.get(e.getNode());
      if (step != null && !PROCESSED_STATE_SET.contains(step.getState())) {
        // Call Registry to change the state to queued
        log.debug("History client: set pipeline step to QUEUED: {}", step);
        Retry.decorateRunnable(
                RETRY, () -> historyClient.setSubmittedPipelineStepToQueued(step.getKey()))
            .run();
        log.info("Step {} with step key {} as QUEUED", step.getType(), step.getKey());
      }
    }
  }

  private boolean containsEvents(I message) {
    PipelineBasedMessage.DatasetInfo datasetInfo = message.getDatasetInfo();
    boolean containsEvents = false;
    if (datasetInfo.getDatasetType() == DatasetType.SAMPLING_EVENT) {
      containsEvents = datasetInfo.isContainsEvents();
    }
    return containsEvents;
  }

  private TrackingInfo trackPipelineStep(I message) throws Exception {

    // create pipeline process. If it already exists it returns the existing one (the db query
    // does an upsert).
    UUID datasetUuid = message.getDatasetUuid();
    Integer attempt = message.getAttempt();

    Supplier<Long> pkSupplier =
        () -> {
          log.debug(
              "History client: create pipeline process, datasetKey {}, attempt {}",
              datasetUuid,
              attempt);
          return historyClient.createPipelineProcess(
              new PipelineProcessParameters(datasetUuid, attempt));
        };

    long processKey = Retry.decorateSupplier(RETRY, pkSupplier).get();

    Long executionId = message.getExecutionId();
    if (executionId == null) {
      log.info("executionId is empty, create initial pipelines execution");
      // create execution
      boolean containsEvents = containsEvents(message);
      boolean containsOccurrences = message.getDatasetInfo().isContainsOccurrences();

      log.info(
          "containsOccurrences: {}, containsEvents: {}, stepType: {}",
          containsOccurrences,
          containsEvents,
          getStepType());

      Set<StepType> stepTypes =
          PipelinesWorkflow.getWorkflow(containsOccurrences, containsEvents)
              .getAllNodesFor(Collections.singleton(getStepType()));

      PipelineExecution execution =
          new PipelineExecution().setStepsToRun(stepTypes).setCreated(OffsetDateTime.now());

      Supplier<Long> executionIdSupplier =
          () -> {
            log.debug(
                "History client: add pipeline execution, processKey {}, execution {}",
                processKey,
                execution);
            return historyClient.addPipelineExecution(processKey, execution);
          };
      executionId = Retry.decorateSupplier(RETRY, executionIdSupplier).get();

      message.setExecutionId(executionId);
    }

    Function<Long, List<PipelineStep>> getStepsByExecutionKeyFn =
        ek -> {
          log.debug("History client: get steps by execution key {}", ek);
          return historyClient.getPipelineStepsByExecutionKey(ek);
        };

    List<PipelineStep> stepsByExecutionKey =
        Retry.decorateFunction(RETRY, getStepsByExecutionKeyFn).apply(executionId);

    // get the step to the process
    PipelineStep step =
        stepsByExecutionKey.stream()
            .filter(ps -> ps.getType() == getStepType())
            .findAny()
            .orElseThrow(
                () ->
                    new PipelinesException(
                        "History service doesn't contain stepType: " + getStepType()));

    // if its in a running state already, it could be it was re-queued after a
    // failure or shutdown
    if (step.getState() != RUNNING && PROCESSED_STATE_SET.contains(step.getState())) {
      log.error(
          "Dataset is in the queue, please check the pipeline-ingestion monitoring tool - {}, running state {}",
          datasetUuid,
          step.getState());
      throw new PipelinesException(
          "Dataset is in the queue, please check the pipeline-ingestion monitoring tool");
    }

    step.setMessage(OBJECT_MAPPER.writeValueAsString(message))
        .setState(RUNNING)
        .setRunner(isStandalone() ? StepRunner.STANDALONE : StepRunner.DISTRIBUTED)
        .setStarted(OffsetDateTime.now())
        .setPipelinesVersion(System.getProperty("pipelinesVersion", "NOT_SET"));

    Function<PipelineStep, Long> pipelineStepFn =
        s -> {
          log.debug("History client: update pipeline step: {}", s);
          return historyClient.updatePipelineStep(s.getKey(), s);
        };
    long stepKey = Retry.decorateFunction(RETRY, pipelineStepFn).apply(step);

    Map<StepType, PipelineStep> pipelineStepMap =
        stepsByExecutionKey.stream()
            .collect(Collectors.toMap(PipelineStep::getType, Function.identity()));

    return TrackingInfo.builder()
        .processKey(processKey)
        .executionId(executionId)
        .pipelineStepMap(pipelineStepMap)
        .stepKey(stepKey)
        .datasetId(datasetUuid.toString())
        .attempt(attempt.toString())
        .build();
  }

  @Builder
  public static class TrackingInfo {
    long processKey;
    long executionId;
    long stepKey;
    String datasetId;
    String attempt;
    Map<StepType, PipelineStep> pipelineStepMap;
  }
}
