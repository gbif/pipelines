package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.coordinator.PrometheusMetrics.*;
import static org.gbif.pipelines.util.CallbackUtil.checkIfPaused;

import feign.Contract;
import feign.Feign;
import feign.auth.BasicAuthRequestInterceptor;
import feign.httpclient.ApacheHttpClient;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.ThreadContext;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.util.CallbackUtil;
import org.gbif.validator.api.Validation;

/**
 * Callback superclass that validator callbacks should extend.
 *
 * @param <I>
 * @param <O>
 */
@Slf4j
public abstract class ValidatorCallback<
        I extends PipelineBasedMessage, O extends PipelineBasedMessage>
    implements CloseableMessageCallback<I> {
  private static final AtomicInteger runningCounter = new AtomicInteger(0);
  protected final CloseableHttpClient httpClient;
  protected final ValidatorStatusService validatorStatusService;
  protected final PipelinesConfig pipelinesConfig;
  protected final MessagePublisher publisher;
  protected final OrchestratorReplies orchestratorReplies;
  protected SparkSession sparkSession;
  protected FileSystem fileSystem;
  protected final String sparkMaster;

  public ValidatorCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String sparkMaster) {

    this.pipelinesConfig = pipelinesConfig;
    this.sparkMaster = sparkMaster;
    this.publisher = publisher;
    this.httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();

    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();

    // initialise Validation client to send status updates
    ValidationClient validationClient =
        Feign.builder()
            // Reuse the timeout-configured http client (60s connect/socket) so a slow or
            // unreachable validation service fails instead of blocking the consumer thread.
            .client(new ApacheHttpClient(httpClient))
            .decoder(new JacksonDecoder(mapper))
            .encoder(new JacksonEncoder(mapper))
            .contract(new Contract.Default())
            .requestInterceptor(
                new BasicAuthRequestInterceptor(
                    pipelinesConfig.getStandalone().getRegistry().getUser(),
                    pipelinesConfig.getStandalone().getRegistry().getPassword()))
            .dismiss404()
            .target(
                ValidationClient.class, pipelinesConfig.getStandalone().getRegistry().getWsUrl());
    this.validatorStatusService = new ValidatorStatusService(validationClient);
    this.orchestratorReplies =
        new OrchestratorReplies(publisher, pipelinesConfig.getOrchestrator());
  }

  protected abstract StepType getStepType();

  protected abstract String getMetaFileName();

  public abstract O createOutgoingMessage(I message);

  protected abstract void runPipeline(I message) throws Exception;

  public abstract Class getMessageClass();

  protected boolean isProcessingStopped(I message) {
    // Validator runs are not tracked in the registry; consult the validation service instead to
    // decide whether the validation has already reached a terminal/non-resumable state. A failure
    // here must not block or crash the consumer, so it is logged and we continue processing.
    try {
      return validatorStatusService.isValidatorAborted(message);
    } catch (Exception e) {
      log.error(
          "Couldn't check validation status for dataset {}, continuing processing",
          message.getDatasetUuid(),
          e);
      return false;
    }
  }

  /** Updates the validation status through the validation service for validator runs. */
  protected void updateValidatorStatus(I message, Validation.Status status, String errorMessage) {
    try {
      log.debug(
          "Updating validation status for dataset {} with step {}",
          message.getDatasetUuid(),
          getStepType().name());
      validatorStatusService.updateStatus(message, getStepType(), status, errorMessage);
    } catch (Exception ex) {
      log.error(
          "Couldn't update validation status for dataset {} with step {}",
          message.getDatasetUuid(),
          getStepType().name(),
          ex);
    }
  }

  public void handleMessage(I message) {
    log.debug("Received message: {}", message);
    runningCounter.incrementAndGet();

    try {
      checkIfPaused();

      LAST_CONSUMED_MESSAGE_FROM_QUEUE_MS.set(System.currentTimeMillis());
      MESSAGES_READ_FROM_QUEUE.inc();

      ThreadContext.put(
          "datasetKey",
          message.getDatasetUuid() != null ? message.getDatasetUuid().toString() : "NO_DATASET");
      log.debug("Received message: {}", message);

      if (isMessageCorrect(message)) {
        Optional<StepHeader> header = OrchestratorReplies.header(message);
        if (header.isPresent()) {
          runForOrchestrator(message, header.get());
          return;
        }
      }

      if (!isMessageCorrect(message) || isProcessingStopped(message)) {
        if (log.isDebugEnabled()) {
          log.debug(
              "Returning message correct: {} isProcessingStopped: {}",
              isMessageCorrect(message),
              isProcessingStopped(message));
        }
        return;
      }

      ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
      ThreadContext.put("attempt", message.getAttempt().toString());
      ThreadContext.put("step", getStepType().name());

      try {
        log.info("Processing step {}", getStepType().name());

        updateValidatorStatus(message, Validation.Status.RUNNING, null);

        CONCURRENT_DATASETS.inc();
        // Run pipeline for this callback
        runPipeline(message);
        COMPLETED_DATASETS.inc();

        updateValidatorStatus(message, Validation.Status.FINISHED, null);

        // set outgoing message to the queue for the next step
        sendOutgoingMessage(message);

        log.info(
            "Finished processing datasetKey: {} with step {}",
            message.getDatasetUuid(),
            getStepType().name());

      } catch (Exception ex) {
        ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
        log.error(
            "Failed to update tracking status for datasetKey - " + message.getDatasetUuid(), ex);
        updateValidatorStatus(message, Validation.Status.FAILED, ex.getMessage());
        DATASETS_ERRORED_COUNT.inc();
        LAST_DATASETS_ERROR.set(System.currentTimeMillis());
      } finally {
        CONCURRENT_DATASETS.dec();
      }
    } catch (Exception e) {
      log.error("Error while processing validation", e);
    } finally {
      runningCounter.decrementAndGet();
    }
  }

  /** What an orchestrated step reports besides its outcome: outputs for later steps and reports. */
  protected record OrchestratorResult(Map<String, String> outputs, List<Report> reports) {
    public static OrchestratorResult none() {
      return new OrchestratorResult(Map.of(), List.of());
    }
  }

  /**
   * Runs before {@link #runPipeline} for a step dispatched by the orchestrator, for example to read
   * the data shape from the step's input instead of from message flags.
   */
  protected void prepareForOrchestrator(I message) throws Exception {}

  /** The outputs and reports of a successful step dispatched by the orchestrator. */
  protected OrchestratorResult orchestratorResult(I message) throws Exception {
    return OrchestratorResult.none();
  }

  /**
   * Runs a step dispatched by the flow-orchestrator: no validator-ws calls and no next message; the
   * result goes back to the orchestrator as a {@link StepCompleted}, also when the step fails.
   */
  private void runForOrchestrator(I message, StepHeader header) {
    ThreadContext.put("datasetKey", message.getDatasetUuid().toString());
    ThreadContext.put("attempt", message.getAttempt().toString());
    ThreadContext.put("step", getStepType().name());
    ThreadContext.put("workflowId", header.workflowId().toString());
    Instant started = Instant.now();
    StepCompleted completed;
    CONCURRENT_DATASETS.inc();
    try {
      log.info(
          "Processing step {} for the orchestrator, step attempt {}",
          getStepType(),
          header.stepAttempt());
      prepareForOrchestrator(message);
      runPipeline(message);
      OrchestratorResult result = orchestratorResult(message);
      COMPLETED_DATASETS.inc();
      completed =
          new StepCompleted(
              UUID.randomUUID(),
              header,
              Outcome.SUCCEEDED,
              null,
              result.outputs(),
              result.reports(),
              started,
              Instant.now());
    } catch (Exception ex) {
      log.error("Step {} failed for the orchestrator", getStepType(), ex);
      DATASETS_ERRORED_COUNT.inc();
      LAST_DATASETS_ERROR.set(System.currentTimeMillis());
      completed =
          new StepCompleted(
              UUID.randomUUID(),
              header,
              Outcome.FAILED,
              diagnostic(ex),
              null,
              null,
              started,
              Instant.now());
    } finally {
      CONCURRENT_DATASETS.dec();
    }
    try {
      orchestratorReplies.send(completed);
    } catch (Exception e) {
      // The step stays dispatched in the orchestrator until its timeout handling (round 6).
      log.error("Could not send the {} completion to the orchestrator", completed.outcome(), e);
    }
  }

  private static String diagnostic(Exception ex) {
    String message = ex.getMessage();
    return ex.getClass().getSimpleName()
        + (message == null || message.isBlank() ? "" : ": " + message);
  }

  protected boolean isMessageCorrect(I message) {
    if (!message.getPipelineSteps().contains(getStepType().name())) {
      log.error(
          "The message doesn't contain {} type, found [size:{}, types: [{}]]",
          getStepType().name(),
          message.getPipelineSteps().size(),
          message.getPipelineSteps().stream().limit(10).collect(Collectors.joining(", ")));
      return false;
    }
    return true;
  }

  private void sendOutgoingMessage(I message) throws IOException {

    // Create and send outgoing message
    O outgoingMessage;
    try {
      outgoingMessage = createOutgoingMessage(message);
    } catch (Exception e) {
      log.error(
          "Failed to create outgoing message for  dataset {}: {}",
          message.getDatasetUuid(),
          e.getMessage(),
          e);
      return;
    }

    if (outgoingMessage == null) {
      log.info(
          "createOutgoingMessage returned null for dataset {}, won't send outgoing message",
          message.getDatasetUuid());
      return;
    }

    if (publisher == null) {
      log.error(
          "Message publisher is null, cannot send outgoing message for dataset {}",
          message.getDatasetUuid());
      return;
    }

    String nextMessageClassName = outgoingMessage.getClass().getSimpleName();
    String messagePayload = outgoingMessage.toString();

    try {
      publisher.send(new PipelinesBalancerMessage(nextMessageClassName, messagePayload));
      log.info(
          "Message sent to balancer for {} step {}",
          outgoingMessage.getDatasetUuid(),
          this.getStepType().name());
    } catch (Exception e) {
      log.error(
          "Failed to send outgoing message for dataset {} after retries: {}",
          message.getDatasetUuid(),
          e.getMessage(),
          e);
    }
  }

  @Override
  public boolean isRunning() {
    return CallbackUtil.isRunning();
  }

  @Override
  public int getRunningCounter() {
    return runningCounter.get();
  }

  @Override
  public void init() throws IOException {

    Configuration hadoopConf = null;
    if (isStandalone()) {
      log.info("Initialising spark session for standalone pipelines");

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

  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {}

  @Override
  public void close() throws Exception {
    this.httpClient.close();
  }

  protected boolean isStandalone() {
    return true;
  }
}
