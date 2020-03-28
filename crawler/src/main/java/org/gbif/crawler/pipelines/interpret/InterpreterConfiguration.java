package org.gbif.crawler.pipelines.interpret;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.crawler.common.configs.AvroWriteConfiguration;
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
 * Configuration required to start Interpretation Pipeline on provided dataset
 */
public class InterpreterConfiguration {

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

  @ParametersDelegate
  @Valid
  @NotNull
  public AvroWriteConfiguration avroConfig = new AvroWriteConfiguration();

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--pool-size")
  @NotNull
  @Min(1)
  public int poolSize;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.VERBATIM_TO_INTERPRETED + ".yml";

  @Parameter(names = "--hdfs-site-config")
  @NotNull
  public String hdfsSiteConfig;

  @Parameter(names = "--core-site-config")
  @NotNull
  public String coreSiteConfig;

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

  @Parameter(names = "--distributed-main-class")
  public String distributedMainClass;

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--standalone-use-java")
  public boolean standaloneUseJava = false;

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
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("messaging", messaging)
        .add("queueName", queueName)
        .add("poolSize", poolSize)
        .add("avroConfig", avroConfig)
        .add("hdfsSiteConfig", hdfsSiteConfig)
        .add("pipelinesConfig", pipelinesConfig)
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
        .add("sparkRecordsPerThread", sparkRecordsPerThread)
        .add("metaFileName", metaFileName)
        .add("processRunner", processRunner)
        .add("sparkParallelismMax", sparkParallelismMax)
        .add("sparkParallelismMin", sparkParallelismMin)
        .add("yarnQueue", yarnQueue)
        .add("registry", registry)
        .toString();
  }
}
