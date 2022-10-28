package org.gbif.pipelines.tasks.verbatims.xml;

import static org.gbif.api.model.pipelines.StepType.XML_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
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
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class XmlToAvroCallbackIT {

  private static final String AVRO = "/verbatim.avro";
  private static final String STRING_UUID = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
  private static final UUID DATASET_UUID = UUID.fromString(STRING_UUID);
  private static final String INPUT_DATASET_FOLDER = "/dataset";
  private static final long EXECUTION_ID = 1L;
  private static final String XML_LABEL = XML_TO_VERBATIM.getLabel();

  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  @Mock private static DatasetClient datasetClient;
  @Mock private static PipelinesHistoryClient historyClient;
  @Mock private static ValidationWsClient validationClient;
  private static CloseableHttpClient httpClient;
  private static ExecutorService executor;

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

    executor = Executors.newSingleThreadExecutor();
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
    executor.shutdown();
  }

  @After
  public void after() {
    publisher.close();
  }

  @Test
  public void testNormalCaseWhenFilesInDirectory() throws Exception {
    // State
    int attempt = 61;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "xml";

    XmlToAvroCallback callback =
        XmlToAvroCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .httpClient(null)
            .datasetClient(datasetClient)
            .build();

    PipelinesXmlMessage message =
        new PipelinesXmlMessage(
            DATASET_UUID,
            attempt,
            20,
            FinishReason.NORMAL,
            Collections.emptySet(),
            EndpointType.BIOCASE_XML_ARCHIVE,
            Platform.PIPELINES,
            EXECUTION_ID);
    String crawlId = DATASET_UUID.toString();

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + STRING_UUID + "/" + attempt + AVRO);
    assertTrue(path.toFile().exists());
    assertTrue(Files.size(path) > 0L);
    assertTrue(checkExists(curator, crawlId, XML_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(XML_LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, XML_LABEL));
  }

  @Test
  public void testFailedCaseWhenXmlAvroEmpty() throws Exception {
    // State
    int attempt = 62;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "xml";

    XmlToAvroCallback callback =
        XmlToAvroCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .httpClient(httpClient)
            .datasetClient(datasetClient)
            .build();

    PipelinesXmlMessage message =
        new PipelinesXmlMessage(
            DATASET_UUID,
            attempt,
            20,
            FinishReason.NORMAL,
            Collections.emptySet(),
            EndpointType.BIOCASE_XML_ARCHIVE,
            Platform.PIPELINES,
            EXECUTION_ID);
    String crawlId = DATASET_UUID.toString();

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + STRING_UUID + "/" + attempt + AVRO);
    assertFalse(path.toFile().exists());
    assertFalse(path.getParent().toFile().exists());
    assertTrue(checkExists(curator, crawlId, XML_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(XML_LABEL)));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, XML_LABEL));
  }

  @Test
  public void testReasonNotNormalCase() {
    // State
    int attempt = 1;
    UUID datasetKey = UUID.randomUUID();
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "xml";

    XmlToAvroCallback callback =
        XmlToAvroCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .httpClient(httpClient)
            .datasetClient(datasetClient)
            .build();

    PipelinesXmlMessage message =
        new PipelinesXmlMessage(
            datasetKey,
            attempt,
            20,
            FinishReason.NOT_MODIFIED,
            Collections.emptySet(),
            EndpointType.BIOCASE_XML_ARCHIVE,
            Platform.PIPELINES,
            EXECUTION_ID);
    String crawlId = datasetKey.toString();

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + crawlId + "/" + attempt + AVRO);
    assertFalse(path.toFile().exists());
    assertFalse(checkExists(curator, crawlId, XML_LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(XML_LABEL)));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }
}
