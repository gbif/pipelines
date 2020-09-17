package org.gbif.pipelines.crawler.dwca;

import static org.gbif.api.model.pipelines.StepType.DWCA_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.crawler.MessagePublisherStub;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/** Test archive-to-avro commands message handling command on hdfs */
public class DwcaToAvroCallbackIT {

  private static final String DWCA_LABEL = StepType.DWCA_TO_VERBATIM.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DUMMY_URL = "http://some.new.url";
  private static final String INPUT_DATASET_FOLDER = "/dataset/dwca";
  private static final long EXECUTION_ID = 1L;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryWsClient historyWsClient;

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

    historyWsClient = Mockito.mock(PipelinesHistoryWsClient.class);
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
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        new DwcaToAvroCallback(config, publisher, curator, historyWsClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID;

    OccurrenceValidationReport report = new OccurrenceValidationReport(1, 1, 0, 1, 0, true);
    DwcaValidationReport reason = new DwcaValidationReport(uuid, report);
    PipelinesDwcaMessage message =
        new PipelinesDwcaMessage(
            uuid,
            DatasetType.OCCURRENCE,
            URI.create(DUMMY_URL),
            attempt,
            reason,
            Collections.emptySet(),
            EndpointType.DWC_ARCHIVE,
            Platform.PIPELINES,
            null);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + DATASET_UUID + "/2/verbatim.avro");
    assertTrue(Files.exists(path));
    assertTrue(Files.size(path) > 0L);
    assertTrue(checkExists(curator, crawlId, DWCA_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(DWCA_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(DWCA_LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(null, null, path.toString());
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, DWCA_LABEL));
  }

  @Test
  public void testNormalSingleStepCase() throws Exception {
    // State
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        new DwcaToAvroCallback(config, publisher, curator, historyWsClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID;

    OccurrenceValidationReport report = new OccurrenceValidationReport(1, 1, 0, 1, 0, true);
    DwcaValidationReport reason = new DwcaValidationReport(uuid, report);
    PipelinesDwcaMessage message =
        new PipelinesDwcaMessage(
            uuid,
            DatasetType.OCCURRENCE,
            URI.create(DUMMY_URL),
            attempt,
            reason,
            Collections.singleton(DWCA_TO_VERBATIM.name()),
            EndpointType.DWC_ARCHIVE,
            Platform.PIPELINES,
            EXECUTION_ID);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + DATASET_UUID + "/2/verbatim.avro");
    assertTrue(Files.exists(path));
    assertTrue(Files.size(path) > 0L);
    assertFalse(checkExists(curator, crawlId, DWCA_LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(DWCA_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(DWCA_LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(null, null, path.toString());
  }

  @Test
  public void testFailedCase() throws Exception {
    // State
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile() + "/1";
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        new DwcaToAvroCallback(config, publisher, curator, historyWsClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID;

    OccurrenceValidationReport report = new OccurrenceValidationReport(1, 1, 0, 1, 0, true);
    DwcaValidationReport reason = new DwcaValidationReport(uuid, report);
    PipelinesDwcaMessage message =
        new PipelinesDwcaMessage(
            uuid,
            DatasetType.OCCURRENCE,
            URI.create(DUMMY_URL),
            attempt,
            reason,
            Collections.emptySet(),
            EndpointType.DWC_ARCHIVE,
            Platform.PIPELINES,
            EXECUTION_ID);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + DATASET_UUID + "/2/verbatim.avro");
    assertFalse(Files.exists(path));
    assertTrue(checkExists(curator, crawlId, DWCA_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(DWCA_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(DWCA_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(DWCA_LABEL)));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, DWCA_LABEL));
  }

  @Test
  public void testInvalidReportStatus() {
    // State
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        new DwcaToAvroCallback(config, publisher, curator, historyWsClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID;

    OccurrenceValidationReport report = new OccurrenceValidationReport(0, 1, 0, 1, 1, true);
    DwcaValidationReport reason = new DwcaValidationReport(uuid, report);
    PipelinesDwcaMessage message =
        new PipelinesDwcaMessage(
            uuid,
            DatasetType.OCCURRENCE,
            URI.create(DUMMY_URL),
            attempt,
            reason,
            Collections.singleton(DWCA_TO_VERBATIM.name()),
            EndpointType.DWC_ARCHIVE,
            Platform.PIPELINES,
            EXECUTION_ID);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + DATASET_UUID + "/2/verbatim.avro");
    assertFalse(Files.exists(path));
    assertFalse(checkExists(curator, crawlId, DWCA_LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(DWCA_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(DWCA_LABEL)));
    assertTrue(publisher.getMessages().isEmpty());
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }
}
