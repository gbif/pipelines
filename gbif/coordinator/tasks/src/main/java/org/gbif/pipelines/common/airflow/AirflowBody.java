package org.gbif.pipelines.common.airflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class AirflowBody {

  @JsonProperty("dag_run_id")
  private final String dagRunId;

  private final Conf conf;

  @Data
  @Builder
  public static class Conf {

    private final List<String> args;

    private final String driverMinCpu;
    private final String driverMaxCpu;
    private final String driverLimitMemory;
    // Sum of driver + vector containers request memory
    private final String driverMinResourceMemory;
    // Sum of driver + vector containers request cpu
    private final String driverMinResourceCpu;

    private final String memoryOverhead;
    private final String executorMinCpu;
    private final String executorMaxCpu;
    private final String executorLimitMemory;
    // Sum of memoryOverhead + executorLimitMemory + vector request memory
    private final String executorMinResourceMemory;
    // Sum of executor + vector containers request cpu
    private final String executorMinResourceCpu;
    private final int minExecutors;
    private final int maxExecutors;
    private final int initialExecutors;
  }
}
