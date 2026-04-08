package org.gbif.pipelines.airflow;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelineStep.Status;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.AirflowConfig;

@Slf4j
public class AirflowSparkLauncher {

  private static final Retry AIRFLOW_RETRY =
      Retry.of(
          "airflowApiCall",
          RetryConfig.custom()
              .maxAttempts(20)
              .retryExceptions(
                  JsonParseException.class,
                  IOException.class,
                  TimeoutException.class,
                  PipelinesException.class)
              .intervalFunction(
                  IntervalFunction.ofExponentialBackoff(
                      Duration.ofSeconds(1), 2d, Duration.ofSeconds(40)))
              .build());

  private final AirflowClient airflowClient;
  private final AirflowConfig airflowConfig;
  private final AirflowConfFactory.Conf conf;
  private final String sparkAppName;

  @Builder
  public AirflowSparkLauncher(
      AirflowConfig airflowConfig,
      AirflowConfFactory.Conf conf,
      String sparkAppName,
      String dagName) {
    this.airflowConfig = airflowConfig;
    this.sparkAppName = sparkAppName;
    this.conf = conf;
    this.airflowClient =
        AirflowClient.builder().config(this.airflowConfig).dagName(dagName).build();
  }

  private AirflowBody getAirflowBody(String dagId, AirflowConfFactory.Conf conf) {
    return AirflowBody.builder().conf(conf).dagRunId(dagId).build();
  }

  public Optional<Status> submitAwait() {
    try {
      AirflowBody airflowBody = getAirflowBody(sparkAppName, conf);

      log.info("Running Airflow DAG ID {}: {}", airflowBody.getDagRunId(), airflowBody);
      Retry.decorateFunction(AIRFLOW_RETRY, airflowClient::createRun).apply(airflowBody);

      Optional<Status> status = getStatusByName(sparkAppName);

      log.info("Waiting Airflow DAG ID {} to finish", airflowBody.getDagRunId());
      while (status.isPresent()
          && Status.COMPLETED != status.get()
          && Status.FAILED != status.get()) {
        TimeUnit.SECONDS.sleep(airflowConfig.apiCheckDelaySec);
        status = getStatusByName(sparkAppName);
      }
      log.info(
          "Airflow DAG ID {} is finished with status - {}",
          airflowBody.getDagRunId(),
          status.orElse(Status.FAILED));

      return status;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      return Optional.of(Status.FAILED);
    }
  }

  public void submitAwaitVoid() {
    Optional<Status> status = submitAwait();
    if (status.isEmpty() || status.get() == Status.ABORTED || status.get() == Status.FAILED) {
      status.ifPresent(s -> log.warn(s.toString()));
      throw new IllegalStateException(
          "Process failed in distributed Job. Check K8s logs " + sparkAppName);
    } else {
      log.info("Process has been finished, Spark job name - {}", sparkAppName);
    }
  }

  @SneakyThrows
  public Optional<Status> getStatusByName(String dagId) {
    JsonNode jsonStatus = Retry.decorateFunction(AIRFLOW_RETRY, airflowClient::getRun).apply(dagId);
    String status = jsonStatus.get("state").asText();

    if ("queued".equalsIgnoreCase(status)
        || "scheduled".equalsIgnoreCase(status)
        || "up_for_reschedule".equalsIgnoreCase(status)
        || "deferred".equalsIgnoreCase(status)) {
      return Optional.of(Status.QUEUED);
    }
    if ("running".equalsIgnoreCase(status)
        || "rescheduled".equalsIgnoreCase(status)
        || "restarting".equalsIgnoreCase(status)
        || "up_for_retry".equalsIgnoreCase(status)
        || "retry".equalsIgnoreCase(status)) {
      return Optional.of(Status.RUNNING);
    }
    if ("success".equalsIgnoreCase(status)) {
      return Optional.of(Status.COMPLETED);
    }
    if ("failed".equalsIgnoreCase(status) || "upstream_failed".equalsIgnoreCase(status)) {
      return Optional.of(Status.FAILED);
    }
    return Optional.empty();
  }
}
