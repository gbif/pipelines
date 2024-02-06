package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import java.util.Collections;
import java.util.Set;
import lombok.ToString;

@ToString
public class SparkConfiguration {

  @Parameter(names = "--spark-records-per-thread")
  public int recordsPerThread;

  @Parameter(names = "--spark-parallelism-min")
  public int parallelismMin;

  @Parameter(names = "--spark-parallelism-max")
  public int parallelismMax;

  @Parameter(names = "--spark-executor-memory-gb-min")
  public int executorMemoryGbMin;

  @Parameter(names = "--spark-executor-memory-gb-max")
  public int executorMemoryGbMax;

  @Parameter(names = "--spark-executor-numbers-min")
  public int executorNumbersMin;

  @Parameter(names = "--spark-executor-numbers-max")
  public int executorNumbersMax;

  @Parameter(names = "--max-record-limit")
  public long maxRecordsLimit;

  @Parameter(names = "--power-fn-coefficient")
  public double powerFnCoefficient;

  @Parameter(names = "--power-fn-exponent")
  public double powerFnExponent;

  @Parameter(names = "--power-fn-executor-coefficient")
  public double powerFnExecutorCoefficient;

  @Parameter(names = "--power-fn-parallelism-coefficient")
  public double powerFnParallelismCoef;

  @Parameter(names = "--power-fn-memory-coefficient")
  public double powerFnMemoryCoef;

  // Some datasets can require more memory because of the size of data fields
  @Parameter(names = "--memory-extra-coef")
  public double memoryExtraCoef = 1d;

  @Parameter(names = "--extra-coef-dataset-list")
  public Set<String> extraCoefDatasetSet = Collections.emptySet();
}
