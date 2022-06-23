package org.gbif.pipelines.tasks.occurrences.hdfs;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.hdfs.HdfsViewConfiguration;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.utils.ZkServer;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

public class OccurrenceHdfsViewCallbackIT {

  private static final String LABEL = StepType.HDFS_VIEW.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryClient historyClient;

  @ClassRule public static final ZkServer ZK_SERVER = new ZkServer();

  @BeforeClass
  public static void setUp() throws Exception {

    server = new TestingServer();
    curator =
        CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .namespace("crawler")
            .retryPolicy(new RetryOneTime(1))
            .build();
    curator.start();

    publisher = MessagePublisherStub.create();

    historyClient = Mockito.mock(PipelinesHistoryClient.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    curator.close();
    server.stop();
    publisher.close();
  }

  @After
  public void after() {
    publisher.close();
  }

  @Test
  public void testNormalCase() throws Exception {
    // State
    HdfsViewConfiguration config = createConfig();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    OccurrenceHdfsViewCallback callback =
        new OccurrenceHdfsViewCallback(config, publisher, curator, historyClient, executor);

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
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());

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
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testWrongRunnerCase() {
    // State
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    HdfsViewConfiguration config = createConfig();
    config.processRunner = StepRunner.DISTRIBUTED.name(); // Message type is STANDALONE

    ExecutorService executor = Executors.newSingleThreadExecutor();

    OccurrenceHdfsViewCallback callback =
        new OccurrenceHdfsViewCallback(config, publisher, curator, historyClient, executor);

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, DATASET_UUID, LABEL));
    assertTrue(publisher.getMessages().isEmpty());
  }

  @Test
  public void testWrongMessageSettingsCase() {
    // State
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    HdfsViewConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    OccurrenceHdfsViewCallback callback =
        new OccurrenceHdfsViewCallback(config, publisher, curator, historyClient, executor);

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setOnlyForStep(StepType.HDFS_VIEW.name()); // Wrong type

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, DATASET_UUID, LABEL));
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
    config.stepConfig.zooKeeper.namespace = curator.getNamespace();
    config.stepConfig.zooKeeper.connectionString = ZK_SERVER.getZkServer().getConnectString();

    config.pipelinesConfig = this.getClass().getClassLoader().getResource("lock.yaml").getPath();
    config.repositoryTargetPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();
    config.recordType = PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;
    return config;
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }
}
