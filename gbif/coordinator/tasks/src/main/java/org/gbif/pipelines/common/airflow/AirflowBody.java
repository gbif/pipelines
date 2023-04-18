package org.gbif.pipelines.common.airflow;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.gbif.pipelines.common.configs.AirflowConfiguration;

@Getter
@Setter
public class AirflowBody {
  private String main;
  private List<String> args;
  private String driverCores;
  private String driverMemory;
  private int executorInstances;
  private String executorCores;
  private String executorMemory;
  private String clusterName;

  public AirflowBody(AirflowConfiguration conf) {
    Optional.ofNullable(conf.airflowCluster).ifPresent(x -> clusterName = x);
    Optional.ofNullable(conf.maxCoresDriver).ifPresent(x -> driverCores = x);
    Optional.ofNullable(conf.maxMemoryDriver).ifPresent(x -> driverMemory = x);
    Optional.ofNullable(conf.maxCoresWorkers).ifPresent(x -> executorCores = x);
    Optional.ofNullable(conf.maxMemoryWorkers).ifPresent(x -> executorMemory = x);
    Optional.of(conf.numberOfWorkers).ifPresent(x -> executorInstances = x);
    main = "";
    args = new ArrayList<>();
  }
}
