package org.gbif.crawler.pipelines.xml;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.crawler.common.utils.HdfsUtils;
import org.gbif.crawler.common.utils.ZookeeperUtils;
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

import static org.gbif.api.model.pipelines.StepType.XML_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore("CLASSPATH ISSUE MUST FIXED")
public class XmlToAvroCallbackTest {

  private static final String AVRO = "/verbatim.avro";
  private static final String STRING_UUID = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
  private static final UUID DATASET_UUID = UUID.fromString(STRING_UUID);
  private static final String INPUT_DATASET_FOLDER = "dataset";
  private static final long EXECUTION_ID = 1L;

  private static final Configuration CONFIG = new Configuration();
  private static String hdfsUri;
  private static MiniDFSCluster cluster;
  private static FileSystem clusterFs;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryWsClient historyWsClient;
  private static ExecutorService executor;

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

    executor = Executors.newSingleThreadExecutor();

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
    executor.shutdown();
  }

  @Test
  public void testNormalCaseWhenFilesInDirectory() throws Exception {
    // State
    int attempt = 61;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = Collections.singleton("xml");
    XmlToAvroCallback callback = new XmlToAvroCallback(config, publisher, curator, historyWsClient, executor);
    PipelinesXmlMessage message =
        new PipelinesXmlMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL, Collections.emptySet(),
                                EndpointType.BIOCASE_XML_ARCHIVE, Platform.PIPELINES, EXECUTION_ID);
    String crawlId = DATASET_UUID + "_" + attempt;

    // When
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + STRING_UUID + "/" + attempt + AVRO);
    assertTrue(cluster.getFileSystem().exists(path));
    assertTrue(clusterFs.getFileStatus(path).getLen() > 0);
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, XML_TO_VERBATIM.getLabel())));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(XML_TO_VERBATIM.getLabel()))));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(null, path.toString());
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, XML_TO_VERBATIM.getLabel()));
    publisher.close();
  }

  @Test
  public void testNormalCaseWhenFilesInTarArchive() throws Exception {
    // State
    int attempt = 60;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = Collections.singleton("xml");
    XmlToAvroCallback callback = new XmlToAvroCallback(config, publisher, curator, historyWsClient, executor);
    PipelinesXmlMessage message =
        new PipelinesXmlMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL, Collections.emptySet(),
                                EndpointType.BIOCASE_XML_ARCHIVE, Platform.PIPELINES, EXECUTION_ID);
    String crawlId = DATASET_UUID + "_" + attempt;

    // When
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + STRING_UUID + "/" + attempt + AVRO);
    assertTrue(cluster.getFileSystem().exists(path));
    assertTrue(clusterFs.getFileStatus(path).getLen() > 0);
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, XML_TO_VERBATIM.getLabel())));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(XML_TO_VERBATIM.getLabel()))));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(null, path.toString());
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, XML_TO_VERBATIM.getLabel()));
    publisher.close();
  }

  @Test
  public void testFailedCaseWhenXmlAvroEmpty() throws Exception {
    // State
    int attempt = 62;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = Collections.singleton("xml");
    XmlToAvroCallback callback = new XmlToAvroCallback(config, publisher, curator, historyWsClient, executor);
    PipelinesXmlMessage message =
        new PipelinesXmlMessage(DATASET_UUID, attempt, 20, FinishReason.NORMAL, Collections.emptySet(),
                                EndpointType.BIOCASE_XML_ARCHIVE, Platform.PIPELINES, EXECUTION_ID);
    String crawlId = DATASET_UUID + "_" + attempt;

    // When
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + STRING_UUID + "/" + attempt + AVRO);
    assertFalse(cluster.getFileSystem().exists(path));
    assertFalse(cluster.getFileSystem().exists(path.getParent()));
    // NOTE: If you run this method independently, it will fail, it is normal
    assertTrue(cluster.getFileSystem().exists(path.getParent().getParent()));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, XML_TO_VERBATIM.getLabel())));
    assertTrue(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.ERROR_MESSAGE.apply(XML_TO_VERBATIM.getLabel()))));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(null, path.toString());
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, XML_TO_VERBATIM.getLabel()));
    publisher.close();
  }

  @Test
  public void testReasonNotNormalCase() throws Exception {
    // State
    int attempt = 60;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = INPUT_DATASET_FOLDER;
    config.repositoryPath = hdfsUri;
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = Collections.singleton("xml");
    XmlToAvroCallback callback = new XmlToAvroCallback(config, publisher, curator, historyWsClient, executor);
    PipelinesXmlMessage message =
        new PipelinesXmlMessage(DATASET_UUID, attempt, 20, FinishReason.NOT_MODIFIED, Collections.emptySet(),
                                EndpointType.BIOCASE_XML_ARCHIVE, Platform.PIPELINES, EXECUTION_ID);
    String crawlId = DATASET_UUID + "_" + attempt;

    // When
    callback.handleMessage(message);

    // Should
    Path path = new Path(hdfsUri + STRING_UUID + "/" + attempt + AVRO);
    assertFalse(cluster.getFileSystem().exists(path));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, XML_TO_VERBATIM.getLabel())));
    assertFalse(ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(XML_TO_VERBATIM.getLabel()))));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(null, path.toString());
    publisher.close();
  }

}
