package org.gbif.pipelines.common.process;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.OCCURRENCE;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Consumer;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.stackable.SparkCrd;
import org.gbif.stackable.SparkCrd.Executor;
import org.junit.Assert;
import org.junit.Test;

public class StackableSparkRunnerTest {

  private static final ValidationResult VALIDATION_RESULT =
      new ValidationResult(true, true, false, 100L, null);

  @Test
  public void tests() {

    int fileShards = 10;

    HdfsViewConfiguration config = createConfig();
    PipelinesInterpretedMessage message = createMessage();

    SparkSettings sparkSettings = SparkSettings.create(1_000, 4, 4, 1.0d);

    Consumer<StringJoiner> beamSettings =
        BeamSettings.occurrenceHdfsView(config, message, fileShards);

    StackableSparkRunner sparkRunner =
        StackableSparkRunner.builder()
            .distributedConfig(config.distributedConfig)
            .sparkConfig(config.sparkConfig)
            .kubeConfigFile(config.stackableConfiguration.kubeConfigFile)
            .sparkCrdConfigFile(config.stackableConfiguration.sparkCrdConfigFile)
            .beamConfigFn(beamSettings)
            .sparkAppName(
                config.stepType + "_" + message.getDatasetUuid() + "_" + message.getAttempt())
            .deleteOnFinish(config.stackableConfiguration.deletePodsOnFinish)
            .sparkSettings(sparkSettings)
            .build();

    SparkCrd sparkCrd = sparkRunner.getSparkCrd();

    Executor executor = sparkCrd.getSpec().getExecutor();
    Assert.assertEquals(
        String.valueOf(sparkSettings.getParallelism()),
        sparkCrd.getSpec().getSparkConf().get("spark.default.parallelism"));
    Assert.assertEquals(sparkSettings.getExecutorNumbers(), executor.getInstances());
    Assert.assertEquals(
        sparkSettings.getExecutorMemory() + "Gi",
        executor.getConfig().getResources().getMemory().getLimit());
  }

  private PipelinesInterpretedMessage createMessage() {
    UUID uuid = UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad");
    int attempt = 2;
    int expSize = 1534;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    return new PipelinesInterpretedMessage(
        uuid,
        attempt,
        new HashSet<>(Arrays.asList(StepType.HDFS_VIEW.name(), StepType.FRAGMENTER.name())),
        (long) expSize,
        null,
        StepRunner.DISTRIBUTED.name(),
        true,
        null,
        null,
        endpointType,
        VALIDATION_RESULT,
        Collections.singleton(OCCURRENCE.name()),
        DatasetType.OCCURRENCE);
  }

  private HdfsViewConfiguration createConfig() {
    HdfsViewConfiguration config = new HdfsViewConfiguration();

    String podConfigPath =
        this.getClass().getClassLoader().getResource("ingest/pipelines-spark-pod.yaml").getPath();
    String kubeconfigPath =
        this.getClass().getClassLoader().getResource("ingest/kubeconfig").getPath();

    // Main
    config.standaloneNumberThreads = 1;
    config.processRunner = StepRunner.DISTRIBUTED.name();
    // Step config
    config.stepConfig.coreSiteConfig = podConfigPath;
    config.stepConfig.hdfsSiteConfig = podConfigPath;
    config.pipelinesConfig = podConfigPath;
    config.stepConfig.repositoryPath =
        this.getClass().getClassLoader().getResource("ingest").getPath();

    config.repositoryTargetPath = this.getClass().getClassLoader().getResource("ingest").getPath();
    config.recordType = OCCURRENCE;

    config.stackableConfiguration.sparkCrdConfigFile = podConfigPath;
    config.stackableConfiguration.kubeConfigFile = kubeconfigPath;

    return config;
  }
}
