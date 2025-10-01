package org.gbif.pipelines.common.process;

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
import org.gbif.pipelines.common.airflow.AirflowBody;
import org.gbif.pipelines.common.airflow.AirflowClient;
import org.gbif.pipelines.common.configs.AirflowConfiguration;
import org.gbif.pipelines.common.configs.SparkConfiguration;
import org.gbif.pipelines.common.process.BeamParametersBuilder.BeamParameters;

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

  private final SparkConfiguration sparkStaticConfiguration;
  private final AirflowClient airflowClient;
  private final AirflowConfiguration airflowConfiguration;
  private final SparkDynamicSettings sparkDynamicSettings;
  private final BeamParameters beamParameters;
  private final String sparkAppName;

  @Builder
  public AirflowSparkLauncher(
      SparkConfiguration sparkStaticConfiguration,
      AirflowConfiguration airflowConfiguration,
      SparkDynamicSettings sparkDynamicSettings,
      BeamParameters beamParameters,
      String sparkAppName) {
    this.sparkStaticConfiguration = sparkStaticConfiguration;
    this.airflowConfiguration = airflowConfiguration;
    this.sparkDynamicSettings = sparkDynamicSettings;
    this.beamParameters = beamParameters;
    this.sparkAppName = sparkAppName;
    this.airflowClient = AirflowClient.builder().configuration(airflowConfiguration).build();

    // add the app name to the beam params
    beamParameters.put("appName", sparkAppName);
  }

  private AirflowBody getAirflowBody(String dagId) {

    int driverMemory = sparkStaticConfiguration.driverMemoryLimit * 1024;
    int driverCpu = Integer.parseInt(sparkStaticConfiguration.driverCpuMin.replace("m", ""));
    int executorMemory = sparkDynamicSettings.getExecutorMemory() * 1024;
    int executorCpu = Integer.parseInt(sparkStaticConfiguration.executorCpuMin.replace("m", ""));
    int memoryOverhead = sparkStaticConfiguration.memoryOverheadMb;
    // Given as megabytes (Mi)
    int vectorMemory = sparkStaticConfiguration.vectorMemoryMb;
    // Given as whole CPUs
    int vectorCpu = sparkStaticConfiguration.vectorCpu;
    // Calculate values for Yunikorn annotation
    // Driver
    int driverMinResourceMemory =
        Double.valueOf(Math.ceil((driverMemory + vectorMemory) / 1024d)).intValue();
    int driverMinResourceCpu = driverCpu + vectorCpu;
    // Executor
    int executorMinResourceMemory =
        Double.valueOf(Math.ceil((executorMemory + memoryOverhead + vectorMemory) / 1024d))
            .intValue();
    int executorMinResourceCpu = executorCpu + vectorCpu;

    return AirflowBody.builder()
        .conf(
            AirflowBody.Conf.builder()
                .args(beamParameters.toList())
                // Driver
                .driverMinCpu(sparkStaticConfiguration.driverCpuMin)
                .driverMaxCpu(sparkStaticConfiguration.driverCpuMax)
                .driverLimitMemory(sparkStaticConfiguration.driverMemoryLimit + "Gi")
                .driverMinResourceMemory(driverMinResourceMemory + "Gi")
                .driverMinResourceCpu(driverMinResourceCpu + "m")
                // Executor
                .memoryOverhead(String.valueOf(sparkStaticConfiguration.memoryOverheadMb))
                .executorMinResourceMemory(executorMinResourceMemory + "Gi")
                .executorMinResourceCpu(executorMinResourceCpu + "m")
                .executorMinCpu(sparkStaticConfiguration.executorCpuMin)
                .executorMaxCpu(sparkStaticConfiguration.executorCpuMax)
                .executorLimitMemory(sparkDynamicSettings.getExecutorMemory() + "Gi")
                // dynamicAllocation
                .initialExecutors(sparkDynamicSettings.getExecutorNumbers())
                .minExecutors(sparkStaticConfiguration.executorInstancesMin)
                .maxExecutors(sparkDynamicSettings.getExecutorNumbers())
                // Extra
                .build())
        .dagRunId(dagId)
        .build();
  }

  public Optional<Status> submitAwait() {
    try {
      AirflowBody airflowBody = getAirflowBody(sparkAppName);

      log.info("Running Airflow DAG ID {}: {}", airflowBody.getDagRunId(), airflowBody);
      Retry.decorateFunction(AIRFLOW_RETRY, airflowClient::createRun).apply(airflowBody);

      Optional<Status> status = getStatusByName(sparkAppName);

      log.info("Waiting Airflow DAG ID {} to finish", airflowBody.getDagRunId());
      while (status.isPresent()
          && Status.COMPLETED != status.get()
          && Status.FAILED != status.get()) {
        TimeUnit.SECONDS.sleep(airflowConfiguration.apiCheckDelaySec);
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
