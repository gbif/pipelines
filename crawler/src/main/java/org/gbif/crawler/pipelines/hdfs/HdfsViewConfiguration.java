package org.gbif.crawler.pipelines.hdfs;

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
 * Configuration required to start Hdfs View processing
 */
public class HdfsViewConfiguration {

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

  @Parameter(names = "--hdfs-site-config")
  @NotNull
  public String hdfsSiteConfig;

  @Parameter(names = "--core-site-config")
  @NotNull
  public String coreSiteConfig;

  @Parameter(names = "--repository-path")
  @NotNull
  public String repositoryPath;

  @Parameter(names = "--repository-target-path")
  @NotNull
  public String repositoryTargetPath;

  @Parameter(names = "--meta-file-name")
  public String metaFileName = Pipeline.INTERPRETED_TO_HDFS + ".yml";

  @Parameter(names = "--distributed-main-class")
  public String distributedMainClass;

  @Parameter(names = "--standalone-stack-size")
  public String standaloneStackSize;

  @Parameter(names = "--standalone-heap-size")
  public String standaloneHeapSize;

  @Parameter(names = "--standalone-jar-path")
  public String standaloneJarPath;

  @Parameter(names = "--standalone-main-class")
  public String standaloneMainClass;

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

  @Parameter(names = "--spark-records-per-thread")
  public int sparkRecordsPerThread;

  @Parameter(names = "--standalone-number-threads")
  public Integer standaloneNumberThreads;

  @Parameter(names = "--standalone-use-java")
  public boolean standaloneUseJava = false;

  @Parameter(names = "--deploy-mode")
  public String deployMode;

  @Parameter(names = "--metrics-properties-path")
  public String metricsPropertiesPath;

  @Parameter(names = "--extra-class-path")
  public String extraClassPath;

  @Parameter(names = "--driver-java-options")
  public String driverJavaOptions;

  @Parameter(names = "--distributed-jar-path")
  public String distributedJarPath;

  @Parameter(names = "--other-user")
  public String otherUser;

  @Parameter(names = "--process-error-directory")
  public String processErrorDirectory;

  @Parameter(names = "--process-output-directory")
  public String processOutputDirectory;

  @Parameter(names = "--hdfs-avro-coefficient-ratio")
  public int hdfsAvroCoefficientRatio = 75;

  @Parameter(names = "--hdfs-avro-expected-file-size-in-mb")
  public int hdfsAvroExpectedFileSizeInMb = 300;

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
      .add("coreSiteConfig", coreSiteConfig)
      .add("repositoryPath", repositoryPath)
      .add("hdfsAvroCoefficient", hdfsAvroCoefficientRatio)
      .add("hdfsAvroExpectedFileSizeInMb", hdfsAvroExpectedFileSizeInMb)
      .toString();
  }

}
