package org.gbif.pipelines.common.indexing;

import java.io.IOException;
import org.gbif.pipelines.common.MainSparkSettings;
import org.gbif.pipelines.common.configs.SparkConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;

public class SparkSettings implements MainSparkSettings {

  private final int parallelism;
  private final String executorMemory;
  private final int executorNumbers;

  private SparkSettings(
      SparkConfiguration sparkConfig,
      StepConfiguration stepConfig,
      String fileCountFilePath,
      long recordsNumber)
      throws IOException {
    this.executorNumbers = computeExecutorNumbers(sparkConfig, recordsNumber);
    this.parallelism = computeParallelism(sparkConfig, stepConfig, fileCountFilePath);
    this.executorMemory = computeExecutorMemory(sparkConfig, executorNumbers, recordsNumber);
  }

  public static SparkSettings create(
      SparkConfiguration sparkConfig,
      StepConfiguration stepConfig,
      String fileCountFilePath,
      long recordsNumber)
      throws IOException {
    return new SparkSettings(sparkConfig, stepConfig, fileCountFilePath, recordsNumber);
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
   * Computes the number of thread for spark.default.parallelism, top limit is
   * config.sparkParallelismMax
   */
  private int computeParallelism(
      SparkConfiguration sparkConfig, StepConfiguration stepConfig, String fileCountFilePath)
      throws IOException {
    // Chooses a runner type by calculating number of files

    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(stepConfig.hdfsSiteConfig, stepConfig.coreSiteConfig);
    int count = HdfsUtils.getFileCount(hdfsConfigs, fileCountFilePath);
    count *= 4;
    if (count < sparkConfig.parallelismMin) {
      return sparkConfig.parallelismMin;
    }
    if (count > sparkConfig.parallelismMax) {
      return sparkConfig.parallelismMax;
    }
    return count;
  }

  /**
   * Computes the memory for executor in Gb, where min is sparkConfig.executorMemoryGbMin and max is
   * sparkConfig.executorMemoryGbMax
   */
  private String computeExecutorMemory(
      SparkConfiguration sparkConfig, int sparkExecutorNumbers, long recordsNumber) {
    int size =
        (int)
            Math.ceil(
                (double) recordsNumber
                    / (sparkExecutorNumbers * sparkConfig.recordsPerThread)
                    * 1.6);

    if (size < sparkConfig.executorMemoryGbMin) {
      return Integer.toString(sparkConfig.executorMemoryGbMin);
    }
    if (size > sparkConfig.executorMemoryGbMax) {
      return Integer.toString(sparkConfig.executorMemoryGbMax);
    }
    return Integer.toString(size);
  }

  /**
   * Computes the numbers of executors, where min is .executorNumbersMin and max is
   * sparkConfig.executorNumbersMax
   *
   * <p>500_000d is records per executor
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
