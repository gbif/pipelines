package org.gbif.pipelines.crawler.interpret;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.AvroWriteConfiguration;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;

/**
 * Configuration required to start Interpretation Pipeline on provided dataset
 */
@ToString
public class InterpreterConfiguration implements BaseConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public StepConfiguration stepConfig = new StepConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.VERBATIM_TO_INTERPRETED + ".yml";

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

  @Parameter(names = "--other-user")
  public String otherUser;

  @Parameter(names = "--spark-parallelism-min")
  public int sparkParallelismMin;

  @Parameter(names = "--spark-parallelism-max")
  public int sparkParallelismMax;

  @Parameter(names = "--spark-records-per-thread")
  public int sparkRecordsPerThread;

  @Parameter(names = "--spark-memory-overhead")
  public int sparkMemoryOverhead;

  @Parameter(names = "--spark-executor-memory-gb-min")
  public int sparkExecutorMemoryGbMin;

  @Parameter(names = "--spark-executor-memory-gb-max")
  public int sparkExecutorMemoryGbMax;

  @Parameter(names = "--spark-executor-cores")
  public int sparkExecutorCores;

  @Parameter(names = "--spark-executor-numbers-min")
  public int sparkExecutorNumbersMin;

  @Parameter(names = "--spark-executor-numbers-max")
  public int sparkExecutorNumbersMax;

  @Parameter(names = "--spark-driver-memory")
  public String sparkDriverMemory;

  @Parameter(names = "--distributed-jar-path")
  public String distributedJarPath;

  @Parameter(names = "--distributed-main-class")
  public String distributedMainClass;

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--process-error-directory")
  public String processErrorDirectory;

  @Parameter(names = "--process-output-directory")
  public String processOutputDirectory;

  @Parameter(names = "--driver-java-options")
  public String driverJavaOptions;

  @Parameter(names = "--metrics-properties-path")
  public String metricsPropertiesPath;

  @Parameter(names = "--extra-class-path")
  public String extraClassPath;

  @Parameter(names = "--deploy-mode")
  public String deployMode;

  @Parameter(names = "--process-runner")
  @NotNull
  public String processRunner;

  @Parameter(names = "--yarn-queue")
  public String yarnQueue;

  @Parameter(names = "--delete-after-days")
  public long deleteAfterDays = 7L;

  @Override
  public String getHdfsSiteConfig() {
    return stepConfig.hdfsSiteConfig;
  }

  @Override
  public String getCoreSiteConfig() {
    return stepConfig.coreSiteConfig;
  }

  @Override
  public String getRepositoryPath() {
    return stepConfig.repositoryPath;
  }

  @Override
  public String getMetaFileName() {
    return metaFileName;
  }

}
