package org.gbif.pipelines.common.configs;

import com.beust.jcommander.Parameter;
import java.util.Collections;
import java.util.Set;
import lombok.ToString;

@ToString
public class SparkConfiguration {

  // Driver settings

  @Parameter(names = "--spark-driver-cpu-min")
  public String driverCpuMin;

  @Parameter(names = "--spark-driver-cpu-max")
  public String driverCpuMax;

  @Parameter(names = "--spark-driver-cpu-max")
  public int driverMemoryLimit;

  // Executor settings

  @Parameter(names = "--memory-overhead-mb")
  public int memoryOverheadMb;

  @Parameter(names = "--spark-executor-cpu-min")
  public String executorCpuMin;

  @Parameter(names = "--spark-executor-cpu-max")
  public String executorCpuMax;

  @Parameter(names = "--spark-executor-memory-gb-min")
  public int executorMemoryGbMin;

  @Parameter(names = "--spark-executor-memory-gb-max")
  public int executorMemoryGbMax;

  @Parameter(names = "--spark-executor-instances-min")
  public int executorInstancesMin;

  @Parameter(names = "--spark-executor-instances-max")
  public int executorInstancesMax;

  // Sidecar pod settings

  @Parameter(names = "--vector-memory-mb")
  public int vectorMemoryMb;

  @Parameter(names = "--vector-cpu")
  public int vectorCpu;

  // Dynamic settings section

  @Parameter(names = "--power-fn-coefficient")
  public double powerFnCoefficient;

  @Parameter(names = "--power-fn-exponent")
  public double powerFnExponent;

  @Parameter(names = "--power-fn-executor-coefficient")
  public double powerFnExecutorCoefficient;

  @Parameter(names = "--power-fn-memory-coefficient")
  public double powerFnMemoryCoef;

  // Some datasets can require more memory because of the size of data fields
  @Parameter(names = "--memory-extra-coef")
  public double memoryExtraCoef = 1d;

  @Parameter(names = "--extra-coef-dataset-list")
  public Set<String> extraCoefDatasetSet = Collections.emptySet();
}
