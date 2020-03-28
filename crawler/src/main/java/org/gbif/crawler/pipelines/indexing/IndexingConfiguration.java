package org.gbif.crawler.pipelines.indexing;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.configs.RegistryConfiguration;
import org.gbif.crawler.common.configs.ZooKeeperConfiguration;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.MoreObjects;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration required to start Indexing Pipeline on provided dataset
 */
public class IndexingConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @ParametersDelegate
  @NotNull
  @Valid
  public RegistryConfiguration registry = new RegistryConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--pool-size")
  @NotNull
  @Min(1)
  public int poolSize;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.INTERPRETED_TO_INDEX + ".yml";

  @Parameter(names = "--hdfs-site-config")
  @NotNull
  public String hdfsSiteConfig;

  @Parameter(names = "--core-site-config")
  @NotNull
  public String coreSiteConfig;

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

  @Parameter(names = "--standalone-stack-size")
  public String standaloneStackSize;

  @Parameter(names = "--standalone-heap-size")
  public String standaloneHeapSize;

  @Parameter(names = "--distributed-jar-path")
  public String distributedJarPath;

  @Parameter(names = "--standalone-jar-path")
  public String standaloneJarPath;

  @Parameter(names = "--standalone-main-class")
  public String standaloneMainClass;

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--standalone-use-java")
  public boolean standaloneUseJava = false;

  @Parameter(names = "--distributed-main-class")
  public String distributedMainClass;

  @Parameter(names = "--repository-path")
  public String repositoryPath;

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

  @Parameter(names = "--index-def-static-prefix-name")
  @NotNull
  public String indexDefStaticPrefixName;

  @Parameter(names = "--index-def-static-date-duration-dd")
  @NotNull
  public Integer indexDefStaticDateDurationDd;

  @Parameter(names = "--index-def-dynamic-prefix-name")
  @NotNull
  public String indexDefDynamicPrefixName;

  @Parameter(names = "--index-def-size")
  @NotNull
  public Integer indexDefSize;

  @Parameter(names = "--index-def-new-if-size")
  @NotNull
  public Integer indexDefNewIfSize;

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
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("messaging", messaging)
        .add("queueName", queueName)
        .add("poolSize", poolSize)
        .add("hdfsSiteConfig", hdfsSiteConfig)
        .add("otherUser", otherUser)
        .add("standaloneStackSize", standaloneStackSize)
        .add("standaloneHeapSize", standaloneHeapSize)
        .add("sparkMemoryOverhead", sparkMemoryOverhead)
        .add("sparkExecutorMemoryGbMin", sparkExecutorMemoryGbMin)
        .add("sparkExecutorMemoryGbMax", sparkExecutorMemoryGbMax)
        .add("sparkExecutorCores", sparkExecutorCores)
        .add("sparkExecutorNumbersMin", sparkExecutorNumbersMin)
        .add("sparkExecutorNumbersMax", sparkExecutorNumbersMax)
        .add("standaloneJarPath", standaloneJarPath)
        .add("distributedJarPath", distributedJarPath)
        .add("repositoryPath", repositoryPath)
        .add("distributedMainClass", distributedMainClass)
        .add("standaloneMainClass", standaloneMainClass)
        .add("processErrorDirectory", processErrorDirectory)
        .add("processOutputDirectory", processOutputDirectory)
        .add("metricsPropertiesPath", metricsPropertiesPath)
        .add("extraClassPath", extraClassPath)
        .add("driverJavaOptions", driverJavaOptions)
        .add("indexIndepRecord", indexIndepRecord)
        .add("indexDefStaticPrefixName", indexDefStaticPrefixName)
        .add("indexIndepRecord", indexIndepRecord)
        .add("indexDefDynamicPrefixName", indexDefDynamicPrefixName)
        .add("indexDefStaticDate", indexDefStaticDateDurationDd)
        .add("indexRecordsPerShard", indexRecordsPerShard)
        .add("metaFileName", metaFileName)
        .add("processRunner", processRunner)
        .add("sparkParallelismMax", sparkParallelismMax)
        .add("yarnQueue", yarnQueue)
        .add("pipelinesConfig", pipelinesConfig)
        .add("esIndexCatUrl", esIndexCatUrl)
        .add("indexDefNewIfSize", indexDefNewIfSize)
        .toString();
  }

}
