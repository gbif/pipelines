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
  private String version;
  private String jar;
  private String image;
  private List<String> args;
  private String driverCores;
  private String driverMem;
  private int workerCount;
  private String workerCores;
  private String workerMem;
  private String cluster;

  public AirflowBody(AirflowConfiguration conf) {
    Optional.ofNullable(conf.airflowCluster).ifPresent(x -> cluster = x);
    Optional.ofNullable(conf.maxCoresDriver).ifPresent(x -> driverCores = x);
    Optional.ofNullable(conf.maxMemoryDriver).ifPresent(x -> driverMem = x);
    Optional.ofNullable(conf.maxCoresWorkers).ifPresent(x -> workerCores = x);
    Optional.ofNullable(conf.maxMemoryWorkers).ifPresent(x -> workerMem = x);
    Optional.of(conf.numberOfWorkers).ifPresent(x -> workerCount = x);
    main = "some-string";
    version = "some-version";
    jar = "some-jar";
    image = "some-image";
    args = new ArrayList<>();
  }
}
