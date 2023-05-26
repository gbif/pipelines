package org.gbif.pipelines.common.interpretation;

import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.MainSparkSettings;
import org.gbif.pipelines.common.configs.SparkConfiguration;

@Slf4j
public class SparkSettings implements MainSparkSettings {

  private final int parallelism;
  private final String executorMemory;
  private final int executorNumbers;

  private SparkSettings(SparkConfiguration sparkConfig, long fileRecordsNumber) {
    this.executorNumbers = computeExecutorNumbers(sparkConfig, fileRecordsNumber);
    this.parallelism = computeParallelism(sparkConfig, executorNumbers);
    this.executorMemory = computeExecutorMemory(sparkConfig, executorNumbers);
  }

  public static SparkSettings create(SparkConfiguration sparkConfig, long fileRecordsNumber) {
    return new SparkSettings(sparkConfig, fileRecordsNumber);
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public String getExecutorMemory() {
    return executorMemory;
  }

  @Override
  public int getExecutorNumbers() {
    return executorNumbers;
  }

  /**
   * Compute the number of thread for spark.default.parallelism, top limit is
   * config.sparkParallelismMax Remember YARN will create the same number of files
   */
  private int computeParallelism(SparkConfiguration sparkConfig, int executorNumbers) {
    int count = executorNumbers * sparkConfig.executorCores * 2;

    if (count < sparkConfig.parallelismMin) {
      return sparkConfig.parallelismMin;
    }
    if (count > sparkConfig.parallelismMax) {
      return sparkConfig.parallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is config.sparkExecutorMemoryGbMin and max is
   * config.sparkExecutorMemoryGbMax
   */
  private String computeExecutorMemory(SparkConfiguration sparkConfig, int sparkExecutorNumbers) {

    if (sparkExecutorNumbers < sparkConfig.executorMemoryGbMin) {
      return String.valueOf(sparkConfig.executorMemoryGbMin);
    }
    if (sparkExecutorNumbers > sparkConfig.executorMemoryGbMax) {
      return String.valueOf(sparkConfig.executorMemoryGbMax);
    }
    return String.valueOf(sparkExecutorNumbers);
  }

  /**
   * Computes the numbers of executors, where min is config.sparkConfig.executorNumbersMin and max
   * is config.sparkConfig.executorNumbersMax
   */
  private int computeExecutorNumbers(SparkConfiguration sparkConfig, long recordsNumber) {
    int sparkExecutorNumbers =
        (int)
            Math.ceil(
                (double) recordsNumber
                    / (sparkConfig.executorCores * sparkConfig.recordsPerThread));
    if (sparkExecutorNumbers < sparkConfig.executorNumbersMin) {
      return sparkConfig.executorNumbersMin;
    }
    if (sparkExecutorNumbers > sparkConfig.executorNumbersMax) {
      return sparkConfig.executorNumbersMax;
    }
    return sparkExecutorNumbers;
  }
}
