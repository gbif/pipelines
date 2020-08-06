package org.gbif.pipelines.crawler.interpret;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.junit.Test;

public class ProcessRunnerBuilderTest {

  private static final long EXECUTION_ID = 1L;

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
        "spark2-submit --conf spark.default.parallelism=1 --conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false "
            + "--conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn --deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 "
            + "--driver-memory 4G java.jar --datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL "
            + "--runner=SparkRunner --targetPath=tmp --metaFileName=verbatim-to-interpreted.yml --inputPath=verbatim.avro "
            + "--avroCompressionType=SNAPPY --avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml "
            + "--properties=/path/ws.config --endPointType=DWC_ARCHIVE --tripletValid=true --occurrenceIdValid=true --useExtendedRecordId=true";

    InterpreterConfiguration config = new InterpreterConfiguration();
    config.distributedJarPath = "java.jar";
    config.distributedMainClass = "org.gbif.Test";
    config.sparkExecutorMemoryGbMax = 10;
    config.sparkExecutorMemoryGbMin = 1;
    config.sparkExecutorCores = 1;
    config.sparkExecutorNumbersMin = 1;
    config.sparkExecutorNumbersMax = 2;
    config.sparkMemoryOverhead = 1;
    config.avroConfig.compressionType = "SNAPPY";
    config.avroConfig.syncInterval = 1;
    config.pipelinesConfig = "/path/ws.config";
    config.sparkDriverMemory = "4G";
    config.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.stepConfig.repositoryPath = "tmp";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(RecordType.ALL.name());
    Set<String> steps = Collections.singleton(StepType.VERBATIM_TO_INTERPRETED.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            datasetId,
            attempt,
            types,
            steps,
            null,
            EndpointType.DWC_ARCHIVE,
            "something",
            new ValidationResult(true, true, true, null),
            null,
            EXECUTION_ID);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .config(config)
            .message(message)
            .inputPath("verbatim.avro")
            .sparkParallelism(1)
            .sparkExecutorMemory("1G")
            .sparkExecutorNumbers(1)
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
            + "--conf spark.executor.memoryOverhead=1 --conf spark.dynamicAllocation.enabled=false --conf spark.yarn.am.waitTime=360s "
            + "--class org.gbif.Test --master yarn "
            + "--deploy-mode cluster --executor-memory 1G --executor-cores 1 --num-executors 1 --driver-memory 4G java.jar "
            + "--datasetId=de7ffb5e-c07b-42dc-8a88-f67a4465fe3d --attempt=1 --interpretationTypes=ALL --runner=SparkRunner "
            + "--targetPath=tmp --metaFileName=verbatim-to-interpreted.yml --inputPath=verbatim.avro --avroCompressionType=SNAPPY "
            + "--avroSyncInterval=1 --hdfsSiteConfig=hdfs.xml --coreSiteConfig=core.xml --properties=/path/ws.config --endPointType=DWC_ARCHIVE";

    InterpreterConfiguration config = new InterpreterConfiguration();
    config.distributedJarPath = "java.jar";
    config.distributedMainClass = "org.gbif.Test";
    config.sparkExecutorMemoryGbMax = 10;
    config.sparkExecutorMemoryGbMin = 1;
    config.sparkExecutorCores = 1;
    config.sparkExecutorNumbersMin = 1;
    config.sparkExecutorNumbersMax = 2;
    config.sparkMemoryOverhead = 1;
    config.avroConfig.compressionType = "SNAPPY";
    config.avroConfig.syncInterval = 1;
    config.pipelinesConfig = "/path/ws.config";
    config.sparkDriverMemory = "4G";
    config.metricsPropertiesPath = "metrics.properties";
    config.extraClassPath = "logstash-gelf.jar";
    config.driverJavaOptions = "-Dlog4j.configuration=file:log4j.properties";
    config.deployMode = "cluster";
    config.processRunner = StepRunner.DISTRIBUTED.name();
    config.yarnQueue = "pipelines";
    config.otherUser = "user";
    config.stepConfig.hdfsSiteConfig = "hdfs.xml";
    config.stepConfig.coreSiteConfig = "core.xml";
    config.stepConfig.repositoryPath = "tmp";

    UUID datasetId = UUID.fromString("de7ffb5e-c07b-42dc-8a88-f67a4465fe3d");
    int attempt = 1;
    Set<String> types = Collections.singleton(RecordType.ALL.name());
    Set<String> steps = Collections.singleton(StepType.VERBATIM_TO_INTERPRETED.name());
    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            datasetId,
            attempt,
            types,
            steps,
            null,
            EndpointType.DWC_ARCHIVE,
            null,
            null,
            null,
            EXECUTION_ID);

    // Expected
    ProcessBuilder builder =
        ProcessRunnerBuilder.builder()
            .config(config)
            .message(message)
            .inputPath("verbatim.avro")
            .sparkParallelism(1)
            .sparkExecutorMemory("1G")
            .sparkExecutorNumbers(1)
            .build()
            .get();

    String result = builder.command().get(2);

    // Should
    assertEquals(expected, result);
  }
}
