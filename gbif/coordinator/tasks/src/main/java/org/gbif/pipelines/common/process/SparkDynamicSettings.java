package org.gbif.pipelines.common.process;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.common.configs.SparkConfiguration;

@Getter
@Slf4j
public class SparkDynamicSettings {

  private final int executorMemory;
  private final int executorNumbers;
  private final double memoryExtraCoef;

  private SparkDynamicSettings(
      SparkConfiguration sparkConfig, long fileRecordsNumber, boolean useMemoryExtraCoef) {
    this.memoryExtraCoef = useMemoryExtraCoef ? sparkConfig.memoryExtraCoef : 1d;
    this.executorNumbers = computeExecutorNumbers(sparkConfig, fileRecordsNumber);
    this.executorMemory = computeExecutorMemory(sparkConfig, fileRecordsNumber);
  }

  // For testing
  private SparkDynamicSettings(int executorMemory, int executorNumbers, double memoryExtraCoef) {
    this.executorMemory = executorMemory;
    this.executorNumbers = executorNumbers;
    this.memoryExtraCoef = memoryExtraCoef;
  }

  public static SparkDynamicSettings create(
      SparkConfiguration sparkConfig, long fileRecordsNumber, boolean memoryExtraCoef) {
    return new SparkDynamicSettings(sparkConfig, fileRecordsNumber, memoryExtraCoef);
  }

  public static SparkDynamicSettings create(
      int executorMemory, int executorNumbers, double memoryExtraCoef) {
    return new SparkDynamicSettings(executorMemory, executorNumbers, memoryExtraCoef);
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
   * Computes the numbers of executors, where min is config.sparkConfig.executorInstancesMin and max
   * is config.sparkConfig.executorInstancesMax
   */
  private int computeExecutorNumbers(SparkConfiguration sparkConfig, long recordsNumber) {

    int sparkExecutorNumbers =
        computePowerFn(sparkConfig, recordsNumber, sparkConfig.powerFnExecutorCoefficient);

    if (sparkExecutorNumbers < sparkConfig.executorInstancesMin) {
      return sparkConfig.executorInstancesMin;
    }
    if (sparkExecutorNumbers > sparkConfig.executorInstancesMax) {
      return sparkConfig.executorInstancesMax;
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
