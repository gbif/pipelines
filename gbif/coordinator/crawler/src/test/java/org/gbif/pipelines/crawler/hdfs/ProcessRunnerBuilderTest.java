package org.gbif.pipelines.crawler.hdfs;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.junit.Test;

public class ProcessRunnerBuilderTest {

  @Test(expected = NullPointerException.class)
  public void testEmptyRunner() {
    // Should
    ProcessRunnerBuilder.builder().build();
  }

  @Test(expected = NullPointerException.class)
  public void testEmptyParameters() {

    // Should
    ProcessRunnerBuilder.builder().build();
  }

  @Test
  public void testSparkRunnerCommand() {
    // When
    String expected =
        "spark2-submit --conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 "
            + "--conf spark.dynamicAllocation.enabled=false "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
            + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --runner=SparkRunner "
            + "--metaFileName=interpreted-to-hdfs.yml --inputPath=tmp --targetPath=target --hdfsSiteConfig=hdfs.xml "
            + "--coreSiteConfig=core.xml --numberOfShards=10 --properties=/path/ws.config --interpretationTypes=OCCURRENCE "
            + "--experiments=use_deprecated_read";

    HdfsViewConfiguration config = new HdfsViewConfiguration();
    config.distributedConfig.jarPath = "java.jar";
    config.distributedConfig.mainClass = "org.gbif.Test";
    config.sparkConfig.executorMemoryGbMax = 10;
    config.sparkConfig.executorMemoryGbMin = 1;
    config.sparkConfig.executorCores = 1;
    config.sparkConfig.executorNumbersMin = 1;
    config.sparkConfig.executorNumbersMax = 2;
    config.sparkConfig.memoryOverhead = 1;
    config.sparkConfig.driverMemory = "4G";
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.pipelinesConfig = "/path/ws.config";
    config.repositoryTargetPath = "target";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.repositoryPath = "tmp";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    ValidationResult vr = new ValidationResult();

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            datasetId,
            attempt,
            steps,
            null,
            null,
            false,
            null,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            vr,
            Collections.singleton(OCCURRENCE.name()),
            false);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .config(config)
            .message(message)
            .sparkParallelism(1)
            .sparkExecutorMemory("1G")
            .sparkExecutorNumbers(1)
            .numberOfShards(10)
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }

  @Test
  public void testSparkRunnerCommandFull() {
    // When
    String expected =
        "sudo -u user spark2-submit --conf spark.metrics.conf=metrics.properties --conf \"spark.driver.extraClassPath=logstash-gelf.jar\" "
            + "--driver-java-options \"-Dlog4j.configuration=file:log4j.properties\" --queue pipelines --conf spark.default.parallelism=1 "
            + "--conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster "
            + "--executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d "
            + "--attempt=1 --runner=SparkRunner --metaFileName=interpreted-to-hdfs.yml --inputPath=tmp --targetPath=target --hdfsSiteConfig=hdfs.xml "
            + "--coreSiteConfig=core.xml --numberOfShards=10 --properties=/path/ws.config --interpretationTypes=OCCURRENCE "
            + "--experiments=use_deprecated_read";

    HdfsViewConfiguration config = new HdfsViewConfiguration();
    config.distributedConfig.jarPath = "java.jar";
    config.distributedConfig.mainClass = "org.gbif.Test";
    config.sparkConfig.executorMemoryGbMax = 10;
    config.sparkConfig.executorMemoryGbMin = 1;
    config.sparkConfig.executorCores = 1;
    config.sparkConfig.executorNumbersMin = 1;
    config.sparkConfig.executorNumbersMax = 2;
    config.sparkConfig.memoryOverhead = 1;
    config.sparkConfig.driverMemory = "4G";
    config.distributedConfig.metricsPropertiesPath = "metrics.properties";
    config.distributedConfig.extraClassPath = "logstash-gelf.jar";
    config.distributedConfig.driverJavaOptions = "-Dlog4j.configuration=file:log4j.properties";
    config.distributedConfig.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.pipelinesConfig = "/path/ws.config";
    config.repositoryTargetPath = "target";
    config.distributedConfig.yarnQueue = "pipelines";
    config.distributedConfig.otherUser = "user";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.repositoryPath = "tmp";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> steps = Collections.singleton(RecordType.ALL.name());
    ValidationResult vr = new ValidationResult();
    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            datasetId,
            attempt,
            steps,
            100L,
            null,
            false,
            null,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            vr,
            Collections.singleton(OCCURRENCE.name()),
            false);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .config(config)
            .message(message)
            .sparkParallelism(1)
            .sparkExecutorMemory("1G")
            .sparkExecutorNumbers(1)
            .numberOfShards(10)
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }
}
