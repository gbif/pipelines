package org.gbif.pipelines.common.process;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.PipelineStep.Status;
import org.gbif.pipelines.common.airflow.AirflowBody;
import org.gbif.pipelines.common.airflow.AirflowClient;
import org.gbif.pipelines.common.configs.AirflowConfiguration;
import org.gbif.pipelines.common.configs.SparkConfiguration;
import org.gbif.pipelines.common.process.BeamParametersBuilder.BeamParameters;

@Slf4j
public class AirflowSparkLauncher {

  private final SparkConfiguration sparkStaticConfiguration;
  private final AirflowClient airflowRunner;
  private final AirflowConfiguration airflowConfiguration;
  private final SparkSettings sparkDynamicSettings;
  private final BeamParameters beamParameters;
  private final String sparkAppName;

  @Builder
  public AirflowSparkLauncher(
      SparkConfiguration sparkStaticConfiguration,
      AirflowConfiguration airflowConfiguration,
      SparkSettings sparkDynamicSettings,
      BeamParameters beamParameters,
      String sparkAppName) {
    this.sparkStaticConfiguration = sparkStaticConfiguration;
    this.airflowConfiguration = airflowConfiguration;
    this.sparkDynamicSettings = sparkDynamicSettings;
    this.beamParameters = beamParameters;
    this.sparkAppName = sparkAppName;
    this.airflowRunner = AirflowClient.builder().configuration(airflowConfiguration).build();
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

  public Optional<Status> submitAndAwait() {
    try {

      String normalizedAppName = normalize(sparkAppName);
      AirflowBody airflowBody = getAirflowBody(normalizedAppName);

      log.info("Running Airflow DAG ID {}: {}", airflowBody.getDagRunId(), airflowBody);

      airflowRunner.createRun(airflowBody);

      Optional<Status> status = getStatusByName(normalizedAppName);

      log.info("Waiting Airflow DAG ID {} to finish", airflowBody.getDagRunId());
      while (status.isPresent()
          && Status.COMPLETED != status.get()
          && Status.FAILED != status.get()) {
        TimeUnit.SECONDS.sleep(airflowConfiguration.apiCheckDelaySec);
        status = getStatusByName(normalizedAppName);
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

  @SneakyThrows
  public Optional<Status> getStatusByName(String dagId) {
    JsonNode jsonStatus = airflowRunner.getRun(dagId);
    String status = jsonStatus.get("state").asText();
    if ("queued".equalsIgnoreCase(status)) {
      return Optional.of(Status.QUEUED);
    }
    if ("running".equalsIgnoreCase(status)
        || "rescheduled".equalsIgnoreCase(status)
        || "retry".equalsIgnoreCase(status)) {
      return Optional.of(Status.RUNNING);
    }
    if ("success".equalsIgnoreCase(status)) {
      return Optional.of(Status.COMPLETED);
    }
    if ("failed".equalsIgnoreCase(status)) {
      return Optional.of(Status.FAILED);
    }
    return Optional.empty();
  }

  /**
   * A lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.'.
   * Must start and end with an alphanumeric character and its max lentgh is 64 characters.
   */
  private static String normalize(String sparkAppName) {
    String v = sparkAppName.toLowerCase().replace("_to_", "-").replace("_", "-");
    return v.length() > 64 ? v.substring(0, 63) : v;
  }
}
