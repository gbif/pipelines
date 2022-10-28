package org.gbif.pipelines.tasks.occurrences.indexing;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.utils.EsServer;
import org.gbif.pipelines.tasks.utils.ZkServer;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IndexingCallbackIT {

  private static final String LABEL = StepType.INTERPRETED_TO_INDEX.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static CloseableHttpClient httpClient;
  @Mock private static DatasetClient datasetClient;
  @Mock private static PipelinesHistoryClient historyClient;
  @Mock private static ValidationWsClient validationClient;

  @ClassRule public static final EsServer ES_SERVER = new EsServer();

  @ClassRule public static final ZkServer ZK_SERVER = new ZkServer();

  @Before
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

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
    httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();
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
    IndexingConfiguration config = createConfig();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .httpClient(httpClient)
            .executor(executor)
            .datasetClient(datasetClient)
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
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testNormalSingleStepCase() {
    // State
    IndexingConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .httpClient(httpClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

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
    assertFalse(checkExists(curator, crawlId, LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());
  }

  @Test
  public void testFailedCase() throws Exception {
    // State
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 59; // Wrong attempt

    IndexingConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    String crawlId = DATASET_UUID;

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testWrongRunnerCase() {
    // State
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    IndexingConfiguration config = createConfig();
    config.processRunner = StepRunner.DISTRIBUTED.name(); // Message type is STANDALONE

    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, DATASET_UUID, LABEL));
    assertTrue(publisher.getMessages().isEmpty());
  }

  @Test
  public void testFailedDistrebutedCase() throws Exception {
    // State
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    IndexingConfiguration config = createConfig();
    config.processRunner = StepRunner.DISTRIBUTED.name();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    String crawlId = DATASET_UUID;

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setRunner(StepRunner.DISTRIBUTED.name());
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testWrongMessageSettingsCase() {
    // State
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    IndexingConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setOnlyForStep(StepType.HDFS_VIEW.name()); // Wrong type

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, DATASET_UUID, LABEL));
    assertTrue(publisher.getMessages().isEmpty());
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

  private IndexingConfiguration createConfig() {
    IndexingConfiguration config = new IndexingConfiguration();
    // Main
    config.standaloneNumberThreads = 1;
    config.processRunner = StepRunner.STANDALONE.name();
    // Indexing
    config.indexConfig.numberReplicas = 1;
    config.indexConfig.recordsPerShard = 1_000;
    config.indexConfig.bigIndexIfRecordsMoreThan = 10_000;
    config.indexConfig.defaultPrefixName = "default";
    config.indexConfig.defaultSize = 2_000;
    config.indexConfig.defaultNewIfSize = 2_500;
    config.indexConfig.defaultSmallestIndexCatUrl =
        ES_SERVER.getEsConfig().getRawHosts()[0]
            + "/_cat/indices/%s*?v&h=docs.count,index&s=docs.count:asc&format=json";
    config.indexConfig.occurrenceAlias = "occurrence";
    config.indexConfig.occurrenceVersion = "a";
    // ES
    config.esConfig.hosts = ES_SERVER.getEsConfig().getRawHosts();
    // Step config
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    config.pipelinesConfig = this.getClass().getClassLoader().getResource("lock.yaml").getPath();
    config.stepConfig.zooKeeper.namespace = curator.getNamespace();
    config.stepConfig.zooKeeper.connectionString = ZK_SERVER.getZkServer().getConnectString();

    config.stepConfig.repositoryPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();
    return config;
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }
}
