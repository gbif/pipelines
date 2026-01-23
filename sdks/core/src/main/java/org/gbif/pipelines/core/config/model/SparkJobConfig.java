package org.gbif.pipelines.core.config.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkJobConfig implements Serializable {

  // command line args
  private List<String> args;

  // spark settings
  public String driverMemoryOverheadFactor;
  public int driverCores;
  public String executorMemoryOverheadFactor;
  public int executorInstances;
  public int executorCores;
  public int defaultParallelism; // should be same as number of shards

  // kubernetes settings
  public String driverMinCpu;
  public String driverMaxCpu;
  public String driverLimitMemory;

  public String executorMinCpu;
  public String executorMaxCpu;

  public String executorLimitMemory;
}
