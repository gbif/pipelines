package org.gbif.pipelines.tasks.occurrences.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.hdfs.CommonHdfsViewCallback;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.resources.CuratorServer;
import org.gbif.pipelines.tasks.resources.ZkServer;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HdfsViewCallbackIT {

  @ClassRule public static final ZkServer ZK_SERVER = ZkServer.getInstance();
  @ClassRule public static final CuratorServer CURATOR_SERVER = CuratorServer.getInstance();
  private static final String LABEL = StepType.HDFS_VIEW.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static PipelinesHistoryClient historyClient;
  @Mock private static DatasetClient datasetClient;

  @After
  public void after() {
    PUBLISHER.close();
  }

  @Test
  public void testNormalCase() {
    // State
    HdfsViewConfiguration config = createConfig();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    HdfsViewCallback callback =
        HdfsViewCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .curator(CURATOR_SERVER.getCurator())
            .historyClient(historyClient)
            .datasetClient(datasetClient)
            .commonHdfsViewCallback(CommonHdfsViewCallback.create(config, executor))
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);

    // When
    callback.handleMessage(message);

    // Should
    Path path =
        Paths.get(
            config.stepConfig.repositoryPath
                + "/"
                + uuid
                + "/"
                + attempt
                + "/"
                + config.metaFileName);
    assertTrue(path.toFile().exists());
    assertTrue(CURATOR_SERVER.checkExists(crawlId, LABEL));
    assertTrue(CURATOR_SERVER.checkExists(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertTrue(CURATOR_SERVER.checkExists(crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(CURATOR_SERVER.checkExists(crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, PUBLISHER.getMessages().size());

    // Check files
    Predicate<String> prFn =
        str ->
            Files.exists(
                Paths.get(
                    config.repositoryTargetPath
                        + "/"
                        + PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE
                            .name()
                            .toLowerCase()
                        + "/"
                        + "/"
                        + str
                        + "/"
                        + DATASET_UUID
                        + "_"
                        + attempt
                        + ".avro"));

    assertTrue(prFn.test("amplificationtable"));
    assertTrue(prFn.test("chronometricagetable"));
    assertTrue(prFn.test("cloningtable"));
    assertTrue(prFn.test("extendedmeasurementorfacttable"));
    assertTrue(prFn.test("gelimagetable"));
    assertTrue(prFn.test("germplasmaccessiontable"));
    assertTrue(prFn.test("germplasmmeasurementscoretable"));
    assertTrue(prFn.test("germplasmmeasurementtraittable"));
    assertTrue(prFn.test("germplasmmeasurementtrialtable"));
    assertTrue(prFn.test("identificationtable"));
    assertTrue(prFn.test("identifiertable"));
    assertTrue(prFn.test("loantable"));
    assertTrue(prFn.test("materialsampletable"));
    assertTrue(prFn.test("measurementorfacttable"));
    assertTrue(prFn.test("occurrence"));
    assertTrue(prFn.test("permittable"));
    assertTrue(prFn.test("preparationtable"));
    assertTrue(prFn.test("preservationtable"));
    assertTrue(prFn.test("referencetable"));
    assertTrue(prFn.test("resourcerelationshiptable"));

    // Clean
    CURATOR_SERVER.deletePath(crawlId, LABEL);
  }

  @Test
  public void testWrongRunnerCase() {
    // State
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    HdfsViewConfiguration config = createConfig();
    config.processRunner = StepRunner.DISTRIBUTED.name(); // Message type is STANDALONE

    ExecutorService executor = Executors.newSingleThreadExecutor();

    HdfsViewCallback callback =
        HdfsViewCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .curator(CURATOR_SERVER.getCurator())
            .historyClient(historyClient)
            .commonHdfsViewCallback(CommonHdfsViewCallback.create(config, executor))
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(CURATOR_SERVER.checkExists(DATASET_UUID, LABEL));
    assertTrue(PUBLISHER.getMessages().isEmpty());
  }

  @Test
  public void testWrongMessageSettingsCase() {
    // State
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    HdfsViewConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    HdfsViewCallback callback =
        HdfsViewCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .curator(CURATOR_SERVER.getCurator())
            .historyClient(historyClient)
            .datasetClient(datasetClient)
            .commonHdfsViewCallback(CommonHdfsViewCallback.create(config, executor))
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setOnlyForStep(StepType.HDFS_VIEW.name()); // Wrong type

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(CURATOR_SERVER.checkExists(DATASET_UUID, LABEL));
  }

  private PipelinesInterpretedMessage createMessage(UUID uuid, int attempt) {
    PipelinesInterpretedMessage message = new PipelinesInterpretedMessage();
    message.setDatasetUuid(uuid);
    message.setAttempt(attempt);
    message.setEndpointType(EndpointType.DWC_ARCHIVE);
    message.setExecutionId(EXECUTION_ID);
    message.setNumberOfRecords(1L);
    message.setRunner(StepRunner.STANDALONE.name());
    message.setInterpretTypes(Collections.singleton("ALL"));
    message.setPipelineSteps(
        new HashSet<>(
            Arrays.asList(StepType.INTERPRETED_TO_INDEX.name(), StepType.HDFS_VIEW.name())));
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
    config.stepConfig.zooKeeper.namespace = CURATOR_SERVER.getCurator().getNamespace();
    config.stepConfig.zooKeeper.connectionString = ZK_SERVER.getZkServer().getConnectString();

    config.pipelinesConfig = this.getClass().getClassLoader().getResource("lock.yaml").getPath();
    config.repositoryTargetPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();
    config.recordType = PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;
    return config;
  }
}
