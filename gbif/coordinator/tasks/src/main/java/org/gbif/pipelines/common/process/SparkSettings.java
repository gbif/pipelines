package org.gbif.pipelines.common.process;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.configs.SparkConfiguration;

@Getter
@Slf4j
public class SparkSettings {

  private final int parallelism;
  private final int executorMemory;
  private final int executorNumbers;
  private final double memoryExtraCoef;

  private SparkSettings(
      SparkConfiguration sparkConfig, long fileRecordsNumber, boolean useMemoryExtraCoef) {
    this.memoryExtraCoef = useMemoryExtraCoef ? sparkConfig.memoryExtraCoef : 1d;
    this.executorNumbers = computeExecutorNumbers(sparkConfig, fileRecordsNumber);
    this.parallelism = computeParallelism(sparkConfig, fileRecordsNumber);
    this.executorMemory = computeExecutorMemory(sparkConfig, fileRecordsNumber);
  }

  public static SparkSettings create(
      SparkConfiguration sparkConfig, long fileRecordsNumber, boolean memoryExtraCoef) {
    return new SparkSettings(sparkConfig, fileRecordsNumber, memoryExtraCoef);
  }

  /**
   * Compute the number of thread for spark.default.parallelism, top limit is
   * config.sparkParallelismMax
   *
   * <p>YARN will create the same number of files
   *
   * @return even value
   */
  private int computeParallelism(SparkConfiguration sparkConfig, long recordsNumber) {
    int count = computePowerFn(sparkConfig, recordsNumber, sparkConfig.powerFnParallelismCoef);
    count = count % 2 == 0 ? count : count + 1;

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
  private int computeExecutorMemory(SparkConfiguration sparkConfig, long recordsNumber) {
    int memoryInGb = computePowerFn(sparkConfig, recordsNumber, sparkConfig.powerFnMemoryCoef);

    memoryInGb = (int) Math.ceil(memoryInGb * memoryExtraCoef);

    if (memoryInGb < sparkConfig.executorMemoryGbMin) {
      return sparkConfig.executorMemoryGbMin;
    }
    if (memoryInGb > sparkConfig.executorMemoryGbMax) {
      return sparkConfig.executorMemoryGbMax;
    }
    return memoryInGb;
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

  /**
   * Power function with base powerFnCoefficient and specific predifined spark coefficient result =
   * powerFnCoefficient * recordsNumber ^ powerFnExponent * coefficient;
   *
   * <p>where:
   *
   * <p>powerFnCoefficient - base power function coefficient, should be unique for a cluster
   *
   * <p>recordsNumber - dataset size
   *
   * <p>powerFnExponent - base exponent, should be unique for a cluster
   *
   * <p>coefficient - extra coefficient, extra coefficient to control executors, memory and
   * parallelism
   */
  private int computePowerFn(
      SparkConfiguration sparkConfig, long recordsNumber, double coefficient) {
    double result =
        sparkConfig.powerFnCoefficient
            * Math.pow(recordsNumber, sparkConfig.powerFnExponent)
            * coefficient;
    return (int) Math.ceil(result);
  }
}
