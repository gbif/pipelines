package org.gbif.pipelines.common.process;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.configs.SparkConfiguration;

@Getter
@Slf4j
public class SparkSettings {

  private final int parallelism;
  private final String executorMemory;
  private final int executorNumbers;

  private SparkSettings(SparkConfiguration sparkConfig, long fileRecordsNumber) {
    this.executorNumbers = computeExecutorNumbers(sparkConfig, fileRecordsNumber);
    this.parallelism = computeParallelism(sparkConfig, fileRecordsNumber);
    this.executorMemory = computeExecutorMemory(sparkConfig, fileRecordsNumber);
  }

  public static SparkSettings create(SparkConfiguration sparkConfig, long fileRecordsNumber) {
    return new SparkSettings(sparkConfig, fileRecordsNumber);
  }

  /**
   * Compute the number of thread for spark.default.parallelism, top limit is
   * config.sparkParallelismMax Remember YARN will create the same number of files
   */
  private int computeParallelism(SparkConfiguration sparkConfig, long recordsNumber) {
    int count = computePowerFn(sparkConfig, recordsNumber, sparkConfig.powerFnParallelismCoef);

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
  private String computeExecutorMemory(SparkConfiguration sparkConfig, long recordsNumber) {
    int sparkExecutorNumbers =
        computePowerFn(sparkConfig, recordsNumber, sparkConfig.powerFnMemoryCoef);
    if (sparkExecutorNumbers < sparkConfig.executorMemoryGbMin) {
      return sparkConfig.executorMemoryGbMin + "G";
    }
    if (sparkExecutorNumbers > sparkConfig.executorMemoryGbMax) {
      return sparkConfig.executorMemoryGbMax + "G";
    }
    return sparkExecutorNumbers + "G";
  }

  /**
   * Computes the numbers of executors, where min is config.sparkConfig.executorNumbersMin and max
   * is config.sparkConfig.executorNumbersMax
   */
  private int computeExecutorNumbers(SparkConfiguration sparkConfig, long recordsNumber) {

    int sparkExecutorNumbers =
        computePowerFn(sparkConfig, recordsNumber, sparkConfig.powerFnExecutorCoefficient);

    if (sparkExecutorNumbers < sparkConfig.executorNumbersMin) {
      return sparkConfig.executorNumbersMin;
    }
    if (sparkExecutorNumbers > sparkConfig.executorNumbersMax) {
      return sparkConfig.executorNumbersMax;
    }
    return sparkExecutorNumbers;
  }

  /** Power function with base powerFnCoefficient and specific predifined spark coefficient */
  private int computePowerFn(
      SparkConfiguration sparkConfig, long recordsNumber, double coefficient) {
    double result =
        sparkConfig.powerFnCoefficient
            * Math.pow(recordsNumber, sparkConfig.powerFnExponent)
            * coefficient;
    return (int) Math.ceil(result);
  }
}
