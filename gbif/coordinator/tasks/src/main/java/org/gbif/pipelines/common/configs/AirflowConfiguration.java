package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Getter
@Setter
public class AirflowConfiguration {

  public static final String SPARK_DAG_NAME = "gbif_spark_execution_dag";

  @Parameter(names = "--airflow-number-of-workers")
  public int numberOfWorkers;

  @Parameter(names = "--airflow-worker-max-memory")
  public String maxMemoryWorkers;

  @Parameter(names = "--airflow-worker-max-cores")
  public String maxCoresWorkers;

  @Parameter(names = "--airflow-driver-max-memory")
  public String maxMemoryDriver;

  @Parameter(names = "--airflow-driver-max-cores")
  public String maxCoresDriver;

  @Parameter(names = "--airflow-cluster")
  public String airflowCluster;

  @Parameter(names = "--use-airflow")
  public boolean useAirflow;

  @Parameter(names = "--airflow-user")
  public String airflowUser;

  @Parameter(names = "--airflow-pass")
  public String airflowPass;

  @Parameter(names = "--airflow-address")
  public String airflowAddress;

  @JsonIgnore
  public String getBasicAuthString() {
    String stringToEncode = airflowUser + ":" + airflowPass;
    return Base64.getEncoder().encodeToString(stringToEncode.getBytes(StandardCharsets.UTF_8));
  }
}
