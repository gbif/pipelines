package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import lombok.ToString;

@ToString
public class SparkConfiguration {

  @Parameter(names = "--spark-records-per-thread")
  public int recordsPerThread;

  @Parameter(names = "--spark-parallelism-min")
  public int parallelismMin;

  @Parameter(names = "--spark-parallelism-max")
  public int parallelismMax;

  @Parameter(names = "--spark-memory-overhead")
  public int memoryOverhead;

  @Parameter(names = "--spark-executor-memory-gb-min")
  public int executorMemoryGbMin;

  @Parameter(names = "--spark-executor-memory-gb-max")
  public int executorMemoryGbMax;

  @Parameter(names = "--spark-executor-cores")
  public int executorCores;

  @Parameter(names = "--spark-executor-numbers-min")
  public int executorNumbersMin;

  @Parameter(names = "--spark-executor-numbers-max")
  public int executorNumbersMax;

  @Parameter(names = "--spark-driver-cores")
  public int driverCores;

  @Parameter(names = "--spark-driver-memory")
  public String driverMemory;
}
