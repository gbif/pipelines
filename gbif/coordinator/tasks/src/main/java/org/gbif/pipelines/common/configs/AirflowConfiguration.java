package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import lombok.ToString;

@ToString
public class AirflowConfiguration {

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

  @Parameter(names = "--airflow-dag")
  public String airflowDag;

  @Parameter(names = "--use-airflow")
  public boolean useAirflow;

  @Parameter(names = "--airflow-user")
  public String airflowUser;

  @Parameter(names = "--airflow-pass")
  public String airflowPass;

  public String getBasicAuthString() {
    String stringToEncode = airflowUser + ":" + airflowPass;
    return Arrays.toString(
        Base64.getEncoder().encode(stringToEncode.getBytes(StandardCharsets.UTF_8)));
  }
}
