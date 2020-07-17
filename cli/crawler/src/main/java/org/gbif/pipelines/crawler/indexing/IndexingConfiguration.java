package org.gbif.pipelines.crawler.indexing;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.common.configs.StepConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.ToString;

/**
 * Configuration required to start Indexing Pipeline on provided dataset
 */
@ToString
public class IndexingConfiguration implements BaseConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public StepConfiguration stepConfig = new StepConfiguration();

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.INTERPRETED_TO_INDEX + ".yml";

  @Parameter(names = "--other-user")
  public String otherUser;

  @Parameter(names = "--spark-records-per-thread")
  public int sparkRecordsPerThread;

  @Parameter(names = "--spark-parallelism-min")
  public int sparkParallelismMin;

  @Parameter(names = "--spark-parallelism-max")
  public int sparkParallelismMax;

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

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--distributed-main-class")
  public String distributedMainClass;

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

  @Parameter(names = "--es-max-batch-size-bytes")
  public Long esMaxBatchSizeBytes;

  @Parameter(names = "--es-max-batch-size")
  public Long esMaxBatchSize;

  @Parameter(names = "--es-hosts")
  @NotNull
  public String[] esHosts;

  @Parameter(names = "--es-schema-path")
  public String esSchemaPath;

  @Parameter(names = "--index-refresh-interval")
  public String indexRefreshInterval;

  @Parameter(names = "--index-number-replicas")
  public Integer indexNumberReplicas;

  @Parameter(names = "--deploy-mode")
  public String deployMode;

  @Parameter(names = "--index-alias")
  public String indexAlias;

  @Parameter(names = "--index-indep-records")
  @NotNull
  public Integer indexIndepRecord;

  @Parameter(names = "--index-default-prefix-name")
  @NotNull
  public String indexDefaultPrefixName;

  @Parameter(names = "--index-default-size")
  @NotNull
  public Integer indexDefaultSize;

  @Parameter(names = "--index-default-new-if-size")
  @NotNull
  public Integer indexDefaultNewIfSize;

  @Parameter(names = "--es-index-cat-url")
  @NotNull
  public String esIndexCatUrl;

  @Parameter(names = "--index-records-per-shard")
  @NotNull
  public Integer indexRecordsPerShard;

  @Parameter(names = "--process-runner")
  @NotNull
  public String processRunner;

  @Parameter(names = "--yarn-queue")
  public String yarnQueue;

  @Parameter(names = "--pipelines-config")
  @Valid
  @NotNull
  public String pipelinesConfig;

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
