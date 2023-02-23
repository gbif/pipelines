package org.gbif.pipelines.tasks.verbatims.fragmenter;

import static org.gbif.api.model.pipelines.PipelineStep.Status.COMPLETED;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;
import static org.gbif.pipelines.fragmenter.common.TableAssert.assertTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.PipelineStep.MetricInfo;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.factory.FileSystemFactory;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.fragmenter.common.HbaseServer;
import org.gbif.registry.ws.client.DatasetClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
  private static final ValidationResult VALIDATION_RESULT =
      new ValidationResult(true, true, false, 100L, null);

  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static ExecutorService executor;
  @Mock private static DatasetClient datasetClient;

  @ClassRule public static final HbaseServer HBASE_SERVER = new HbaseServer();

  @BeforeClass
  public static void setUp() throws Exception {

    server = new TestingServer();

    publisher = MessagePublisherStub.create();
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterClass
  public static void tearDown() throws IOException {
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
  public void dwcaTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.dwcaArchiveRepository = getClass().getResource(DWCA_INPUT_DATASET_DIR).getFile();
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.asyncThreshold = 1_000;
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString(DWCA_DATASET_UUID);
    int attempt = 2;
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
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertEquals(1, publisher.getMessages().size());
    assertTable(HBASE_SERVER.getConnection(), expSize, DWCA_DATASET_UUID, attempt, endpointType);
    assertMetaFile(config, uuid, attempt, expSize);

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep fragmenterResult = result.get(StepType.FRAGMENTER);
    Assert.assertNotNull(fragmenterResult);
    Assert.assertEquals(COMPLETED, fragmenterResult.getState());
  }

  @Test
  public void abcdTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.xmlArchiveRepository = getClass().getResource(REPO_PATH).getFile();
    config.xmlArchiveRepositoryAbcd = "abcd";
    config.xmlArchiveRepositoryXml = "xml";
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString("830c56c4-57bf-4858-9795-c1f8c7ff9b1e");
    int attempt = 61;
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
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertEquals(1, publisher.getMessages().size());
    assertTable(HBASE_SERVER.getConnection(), expSize, uuid.toString(), attempt, endpointType);
    assertMetaFile(config, uuid, attempt, expSize);

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep fragmenterResult = result.get(StepType.FRAGMENTER);
    Assert.assertNotNull(fragmenterResult);
    Assert.assertEquals(COMPLETED, fragmenterResult.getState());
  }

  @Test
  public void xmlTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.xmlArchiveRepository = getClass().getResource(REPO_PATH).getFile();
    config.xmlArchiveRepositoryAbcd = "abcd";
    config.xmlArchiveRepositoryXml = "xml";
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString("7ef15372-1387-11e2-bb2e-00145eb45e9a");
    int attempt = 61;
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
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertEquals(1, publisher.getMessages().size());
    assertTable(HBASE_SERVER.getConnection(), expSize, uuid.toString(), attempt, endpointType);
    assertMetaFile(config, uuid, attempt, expSize);

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep fragmenterResult = result.get(StepType.FRAGMENTER);
    Assert.assertNotNull(fragmenterResult);
    Assert.assertEquals(COMPLETED, fragmenterResult.getState());
  }

  @Test
  public void wrongMessageTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.hbaseFragmentsTable = HbaseServer.FRAGMENT_TABLE_NAME;
    config.dwcaArchiveRepository = getClass().getResource(DWCA_INPUT_DATASET_DIR).getFile();
    config.stepConfig.repositoryPath = getClass().getResource(REPO_PATH).getFile();
    config.generateIdIfAbsent = true;

    UUID uuid = UUID.fromString(DWCA_DATASET_UUID);
    int attempt = 2;
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
            null,
            endpointType,
            VALIDATION_RESULT,
            Collections.singleton(OCCURRENCE.name()),
            null);

    FragmenterCallback callback =
        FragmenterCallback.builder()
            .config(config)
            .publisher(publisher)
            .historyClient(historyClient)
            .executor(executor)
            .hbaseConnection(HBASE_SERVER.getConnection())
            .keygenConfig(HbaseServer.CFG)
            .datasetClient(datasetClient)
            .build();

    // When
    callback.handleMessage(message);

    // Should
    assertEquals(0, publisher.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());
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
