package org.gbif.pipelines.tasks.verbatims.fragmenter;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;
import static org.gbif.pipelines.fragmenter.common.TableAssert.assertTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.PipelineStep.MetricInfo;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.fragmenter.common.HbaseServer;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FragmenterCallbackIT {

  private static final String DWCA_DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DWCA_INPUT_DATASET_DIR = "/dataset/dwca";
  private static final String REPO_PATH = "/dataset/";
  private static final String FRAGMENTER_LABEL = StepType.FRAGMENTER.getLabel();
  private static final ValidationResult VALIDATION_RESULT =
      new ValidationResult(true, true, false, 0L, null);
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static ExecutorService executor;
  @Mock private PipelinesHistoryClient client;
  @Mock private static DatasetClient datasetClient;

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

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
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    curator.close();
    server.stop();
    publisher.close();
    executor.shutdown();
  }

  @After
  public void after() throws IOException {
    publisher.close();
    HBASE_SERVER.truncateTable();
  }

  @Test
  public void testDwca() throws Exception {
    // State
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.dwcaArchiveRepository = getClass().getResource(DWCA_INPUT_DATASET_DIR).getFile();
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.asyncThreshold = 1_000;
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString(DWCA_DATASET_UUID);
    int attempt = 2;
    String crawlId = DWCA_DATASET_UUID;
    int expSize = 1534;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            uuid,
            attempt,
            new HashSet<>(Arrays.asList(StepType.HDFS_VIEW.name(), StepType.FRAGMENTER.name())),
            (long) expSize,
            null,
            StepRunner.STANDALONE.name(),
            true,
            null,
            null,
            null,
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, FRAGMENTER_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(FRAGMENTER_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertEquals(1, publisher.getMessages().size());
    assertTable(HBASE_SERVER.getConnection(), expSize, DWCA_DATASET_UUID, attempt, endpointType);
    assertMetaFile(config, uuid, attempt, expSize);

    // Clean
    curator
        .delete()
        .deletingChildrenIfNeeded()
        .forPath(getPipelinesInfoPath(crawlId, FRAGMENTER_LABEL));
  }

  @Test
  public void testAbcd() throws Exception {
    // State
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.xmlArchiveRepository = getClass().getResource(REPO_PATH).getFile();
    config.xmlArchiveRepositoryAbcd = "abcd";
    config.xmlArchiveRepositoryXml = "xml";
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString("830c56c4-57bf-4858-9795-c1f8c7ff9b1e");
    int attempt = 61;
    String crawlId = uuid.toString();
    int expSize = 20;
    EndpointType endpointType = EndpointType.BIOCASE_XML_ARCHIVE;

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            uuid,
            attempt,
            new HashSet<>(Arrays.asList(StepType.HDFS_VIEW.name(), StepType.FRAGMENTER.name())),
            (long) expSize,
            null,
            StepRunner.DISTRIBUTED.name(),
            true,
            null,
            null,
            null,
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, FRAGMENTER_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(FRAGMENTER_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertEquals(1, publisher.getMessages().size());
    assertTable(HBASE_SERVER.getConnection(), expSize, uuid.toString(), attempt, endpointType);
    assertMetaFile(config, uuid, attempt, expSize);

    // Clean
    curator
        .delete()
        .deletingChildrenIfNeeded()
        .forPath(getPipelinesInfoPath(crawlId, FRAGMENTER_LABEL));
  }

  @Test
  public void testXml() throws Exception {
    // State
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.xmlArchiveRepository = getClass().getResource(REPO_PATH).getFile();
    config.xmlArchiveRepositoryAbcd = "abcd";
    config.xmlArchiveRepositoryXml = "xml";
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString("7ef15372-1387-11e2-bb2e-00145eb45e9a");
    int attempt = 61;
    String crawlId = uuid.toString();
    int expSize = 20;
    EndpointType endpointType = EndpointType.BIOCASE;

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            uuid,
            attempt,
            new HashSet<>(Arrays.asList(StepType.HDFS_VIEW.name(), StepType.FRAGMENTER.name())),
            (long) expSize,
            null,
            StepRunner.STANDALONE.name(),
            true,
            null,
            null,
            null,
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, FRAGMENTER_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(FRAGMENTER_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertEquals(1, publisher.getMessages().size());
    assertTable(HBASE_SERVER.getConnection(), expSize, uuid.toString(), attempt, endpointType);
    assertMetaFile(config, uuid, attempt, expSize);

    // Clean
    curator
        .delete()
        .deletingChildrenIfNeeded()
        .forPath(getPipelinesInfoPath(crawlId, FRAGMENTER_LABEL));
  }

  @Test
  public void testWrongMessage() {
    // State
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.dwcaArchiveRepository = getClass().getResource(DWCA_INPUT_DATASET_DIR).getFile();
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString(DWCA_DATASET_UUID);
    int attempt = 2;
    String crawlId = DWCA_DATASET_UUID;
    int expSize = 1534;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            uuid,
            attempt,
            Collections.singleton(StepType.HDFS_VIEW.name()),
            (long) expSize,
            null,
            StepRunner.STANDALONE.name(),
            true,
            null,
            StepType.FRAGMENTER.name(),
            null,
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, crawlId, FRAGMENTER_LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(FRAGMENTER_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertEquals(0, publisher.getMessages().size());
  }

  @Test
  public void testOnlyForStep() {
    // State
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.dwcaArchiveRepository = getClass().getResource(DWCA_INPUT_DATASET_DIR).getFile();
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString(DWCA_DATASET_UUID);
    int attempt = 2;
    String crawlId = DWCA_DATASET_UUID;
    int expSize = 1534;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    PipelinesInterpretedMessage message =
        new PipelinesInterpretedMessage(
            uuid,
            attempt,
            Collections.singleton(StepType.HDFS_VIEW.name()),
            (long) expSize,
            null,
            StepRunner.STANDALONE.name(),
            true,
            null,
            StepType.HDFS_VIEW.name(),
            null,
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, crawlId, FRAGMENTER_LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(FRAGMENTER_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertEquals(0, publisher.getMessages().size());
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }

  private void assertMetaFile(
      FragmenterConfiguration config, UUID datasetId, int attempt, int expSize) throws IOException {
    org.apache.hadoop.fs.Path path =
        HdfsUtils.buildOutputPath(
            config.stepConfig.repositoryPath,
            datasetId.toString(),
            String.valueOf(attempt),
            config.metaFileName);
    HdfsConfigs hdfsConfigs =
        HdfsConfigs.create(config.getHdfsSiteConfig(), config.getCoreSiteConfig());
    FileSystem fs = FileSystemFactory.getInstance(hdfsConfigs).getFs(path.toString());
    List<MetricInfo> metricInfos = HdfsUtils.readMetricsFromMetaFile(hdfsConfigs, path.toString());
    assertTrue(fs.exists(path));
    assertEquals(1, metricInfos.size());
    assertEquals(String.valueOf(expSize), metricInfos.get(0).getValue());
  }
}
