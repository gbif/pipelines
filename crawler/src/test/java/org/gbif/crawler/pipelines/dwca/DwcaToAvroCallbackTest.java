package org.gbif.crawler.pipelines.dwca;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.UUID;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.common.utils.HdfsUtils;
import org.gbif.crawler.common.utils.ZookeeperUtils;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.crawler.pipelines.MessagePublisherStub;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.gbif.api.model.pipelines.StepType.DWCA_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test archive-to-avro commands message handling command on hdfs
 */
@Ignore("CLASSPATH ISSUE MUST FIXED")
public class DwcaToAvroCallbackTest {

  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DUMMY_URL = "http://some.new.url";
  private static final String INPUT_DATASET_FOLDER = "dataset/dwca";
  private static final Configuration CONFIG = new Configuration();
  private static final long EXECUTION_ID = 1L;
  private static String hdfsUri;
  private static MiniDFSCluster cluster;
  private static FileSystem clusterFs;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryWsClient historyWsClient;

  @BeforeClass
  public static void setUp() throws Exception {
    File baseDir = new File("minicluster").getAbsoluteFile();
    FileUtil.fullyDelete(baseDir);
    CONFIG.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(CONFIG);
    cluster = builder.build();
    hdfsUri = "hdfs://localhost:" + cluster.getNameNodePort() + "/";
    cluster.waitClusterUp();
    clusterFs = cluster.getFileSystem();

    server = new TestingServer();
    curator = CuratorFrameworkFactory.builder()
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
    clusterFs.close();
    cluster.shutdown();
    curator.close();
    server.stop();
    publisher.close();
  }

  @Test
  public void testNormalCase() throws Exception {
    // State
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;

    DwcaToAvroCallback callback = new DwcaToAvroCallback(config, publisher, curator, historyWsClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID + "_" + attempt;

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
    Path path = new Path(hdfsUri + DATASET_UUID + "/2/verbatim.avro");
    assertTrue(cluster.getFileSystem().exists(path));
    assertTrue(clusterFs.getFileStatus(path).getLen() > 0);
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, DWCA_TO_VERBATIM.getLabel())));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.MQ_CLASS_NAME.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.MQ_MESSAGE.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(null, path.toString());
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, DWCA_TO_VERBATIM.getLabel()));
    publisher.close();
  }

  @Test
  public void testNormalSingleStepCase() throws Exception {
    // State
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;

    DwcaToAvroCallback callback = new DwcaToAvroCallback(config, publisher, curator, historyWsClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID + "_" + attempt;

    OccurrenceValidationReport report = new OccurrenceValidationReport(1, 1, 0, 1, 0, true);
    DwcaValidationReport reason = new DwcaValidationReport(uuid, report);
    PipelinesDwcaMessage message =
        new PipelinesDwcaMessage(uuid, DatasetType.OCCURRENCE, URI.create(DUMMY_URL), attempt, reason,
            Collections.singleton(DWCA_TO_VERBATIM.name()), EndpointType.DWC_ARCHIVE, Platform.PIPELINES, EXECUTION_ID);

    // When
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + DATASET_UUID + "/2/verbatim.avro");
    assertTrue(cluster.getFileSystem().exists(path));
    assertTrue(clusterFs.getFileStatus(path).getLen() > 0);
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, DWCA_TO_VERBATIM.getLabel())));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.MQ_CLASS_NAME.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.MQ_MESSAGE.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(null, path.toString());
    publisher.close();
  }

  @Test
  public void testFailedCase() throws Exception {
    // State
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER + "/1";
    config.repositoryPath = hdfsUri;

    DwcaToAvroCallback callback = new DwcaToAvroCallback(config, publisher, curator, historyWsClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID + "_" + attempt;

    OccurrenceValidationReport report = new OccurrenceValidationReport(1, 1, 0, 1, 0, true);
    DwcaValidationReport reason = new DwcaValidationReport(uuid, report);
    PipelinesDwcaMessage message =
        new PipelinesDwcaMessage(uuid, DatasetType.OCCURRENCE, URI.create(DUMMY_URL), attempt, reason,
            Collections.emptySet(), EndpointType.DWC_ARCHIVE, Platform.PIPELINES, EXECUTION_ID);

    // When
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + DATASET_UUID + "/2/verbatim.avro");
    assertFalse(cluster.getFileSystem().exists(path));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, DWCA_TO_VERBATIM.getLabel())));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.ERROR_MESSAGE.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.MQ_CLASS_NAME.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.MQ_MESSAGE.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(null, path.toString());
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, DWCA_TO_VERBATIM.getLabel()));
    publisher.close();
  }

  @Test
  public void testInvalidReportStatus() throws IOException {
    // State
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;

    DwcaToAvroCallback callback = new DwcaToAvroCallback(config, publisher, curator, historyWsClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID + "_" + attempt;

    OccurrenceValidationReport report = new OccurrenceValidationReport(0, 1, 0, 1, 1, true);
    DwcaValidationReport reason = new DwcaValidationReport(uuid, report);
    PipelinesDwcaMessage message =
        new PipelinesDwcaMessage(uuid, DatasetType.OCCURRENCE, URI.create(DUMMY_URL), attempt, reason,
            Collections.singleton(DWCA_TO_VERBATIM.name()), EndpointType.DWC_ARCHIVE, Platform.PIPELINES, EXECUTION_ID);

    // When
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + DATASET_UUID + "/2/verbatim.avro");
    assertFalse(cluster.getFileSystem().exists(path));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, DWCA_TO_VERBATIM.getLabel())));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.MQ_CLASS_NAME.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.MQ_MESSAGE.apply(DWCA_TO_VERBATIM.getLabel()))));
    assertTrue(publisher.getMessages().isEmpty());

    publisher.close();
  }

}
