package org.gbif.pipelines.tasks.occurrences.hdfs;

import static org.gbif.api.model.pipelines.PipelineStep.Status.COMPLETED;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.hdfs.CommonHdfsViewCallback;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.PipelinesHistoryClientTestStub;
import org.gbif.pipelines.tasks.resources.ZkServer;
import org.gbif.registry.ws.client.DatasetClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HdfsViewCallbackIT {

  @ClassRule public static final ZkServer ZK_SERVER = ZkServer.getInstance();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static DatasetClient datasetClient;

  @After
  public void after() {
    PUBLISHER.close();
  }

  @Test
  public void successFullInterpretationTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    HdfsViewConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    HdfsViewCallback callback =
        HdfsViewCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .datasetClient(datasetClient)
            .commonHdfsViewCallback(CommonHdfsViewCallback.create(config, executor))
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);

    // When
    callback.handleMessage(message);

    // Should
    String repositoryPath = config.stepConfig.repositoryPath;
    Path path = Paths.get(repositoryPath + "/" + uuid + "/" + attempt + "/" + config.metaFileName);
    assertTrue(path.toFile().exists());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep hdfsViewResult = result.get(StepType.HDFS_VIEW);
    Assert.assertNotNull(hdfsViewResult);
    Assert.assertEquals(COMPLETED, hdfsViewResult.getState());

    // Check files
    Function<String, Path> prFn =
        str ->
            Paths.get(
                config.repositoryTargetPath
                    + "/"
                    + OCCURRENCE.name().toLowerCase()
                    + "//"
                    + str
                    + "/"
                    + DATASET_UUID
                    + "_"
                    + attempt
                    + ".avro");

    assertTrue(Files.exists(prFn.apply("occurrence")));

    assertFalse(Files.exists(prFn.apply("amplificationtable")));
    assertFalse(Files.exists(prFn.apply("chronometricagetable")));
    assertFalse(Files.exists(prFn.apply("cloningtable")));
    assertFalse(Files.exists(prFn.apply("extendedmeasurementorfacttable")));
    assertFalse(Files.exists(prFn.apply("gelimagetable")));
    assertFalse(Files.exists(prFn.apply("germplasmaccessiontable")));
    assertFalse(Files.exists(prFn.apply("germplasmmeasurementscoretable")));
    assertFalse(Files.exists(prFn.apply("germplasmmeasurementtraittable")));
    assertFalse(Files.exists(prFn.apply("germplasmmeasurementtrialtable")));
    assertFalse(Files.exists(prFn.apply("identificationtable")));
    assertFalse(Files.exists(prFn.apply("identifiertable")));
    assertFalse(Files.exists(prFn.apply("loantable")));
    assertFalse(Files.exists(prFn.apply("materialsampletable")));
    assertFalse(Files.exists(prFn.apply("measurementorfacttable")));
    assertFalse(Files.exists(prFn.apply("permittable")));
    assertFalse(Files.exists(prFn.apply("preparationtable")));
    assertFalse(Files.exists(prFn.apply("preservationtable")));
    assertFalse(Files.exists(prFn.apply("referencetable")));
    assertFalse(Files.exists(prFn.apply("resourcerelationshiptable")));
  }

  @Test
  public void wrongRunnerCaseTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    HdfsViewConfiguration config = createConfig();
    config.processRunner = StepRunner.DISTRIBUTED.name(); // Message type is STANDALONE

    ExecutorService executor = Executors.newSingleThreadExecutor();

    HdfsViewCallback callback =
        HdfsViewCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .commonHdfsViewCallback(CommonHdfsViewCallback.create(config, executor))
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(PUBLISHER.getMessages().isEmpty());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());
  }

  private PipelinesInterpretedMessage createMessage(UUID uuid, int attempt) {
    PipelinesInterpretedMessage message = new PipelinesInterpretedMessage();
    message.setDatasetUuid(uuid);
    message.setAttempt(attempt);
    message.setEndpointType(EndpointType.DWC_ARCHIVE);
    message.setNumberOfRecords(1L);
    message.setRunner(StepRunner.STANDALONE.name());
    message.setInterpretTypes(Collections.singleton("ALL"));
    message.setPipelineSteps(
        new HashSet<>(
            Arrays.asList(StepType.INTERPRETED_TO_INDEX.name(), StepType.HDFS_VIEW.name())));
    message.setValidationResult(new ValidationResult(true, true, false, 1_000L, 0L));
    return message;
  }

  private HdfsViewConfiguration createConfig() {
    HdfsViewConfiguration config = new HdfsViewConfiguration();

    // Main
    config.standaloneNumberThreads = 1;
    config.processRunner = StepRunner.STANDALONE.name();
    // Step config
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    config.stepConfig.repositoryPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();

    config.pipelinesConfig = this.getClass().getClassLoader().getResource("lock.yaml").getPath();
    config.repositoryTargetPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();
    config.recordType = OCCURRENCE;
    return config;
  }
}
