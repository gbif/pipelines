package org.gbif.pipelines.tasks.verbatims.xml;

import static org.gbif.api.model.pipelines.StepType.XML_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesXmlMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.resources.CuratorServer;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class XmlToAvroCallbackIT {

  @ClassRule public static final CuratorServer CURATOR_SERVER = CuratorServer.getInstance();
  private static final String AVRO = "/verbatim.avro";
  private static final String STRING_UUID = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
  private static final UUID DATASET_UUID = UUID.fromString(STRING_UUID);
  private static final String INPUT_DATASET_FOLDER = "/dataset";
  private static final long EXECUTION_ID = 1L;
  private static final String XML_LABEL = XML_TO_VERBATIM.getLabel();
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
  @Mock private PipelinesHistoryClient historyClient;
  @Mock private ValidationWsClient validationClient;
  @Mock private static DatasetClient datasetClient;
  private static final CloseableHttpClient HTTP_CLIENT =
      HttpClients.custom()
          .setDefaultRequestConfig(
              RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
          .build();

  @AfterClass
  public static void tearDown() {
    EXECUTOR.shutdown();
  }

  @After
  public void after() {
    PUBLISHER.close();
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
            .publisher(PUBLISHER)
            .curator(CURATOR_SERVER.getCurator())
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(EXECUTOR)
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
    assertTrue(CURATOR_SERVER.checkExists(crawlId, XML_LABEL));
    assertTrue(CURATOR_SERVER.checkExists(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(XML_LABEL)));
    assertEquals(1, PUBLISHER.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
    CURATOR_SERVER.deletePath(crawlId, XML_LABEL);
  }

  @Test
  public void testFailedCaseWhenXmlAvroEmpty() {
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
            .publisher(PUBLISHER)
            .curator(CURATOR_SERVER.getCurator())
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(EXECUTOR)
            .httpClient(HTTP_CLIENT)
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
    assertTrue(CURATOR_SERVER.checkExists(crawlId, XML_LABEL));
    assertTrue(CURATOR_SERVER.checkExists(crawlId, Fn.ERROR_MESSAGE.apply(XML_LABEL)));
    assertTrue(PUBLISHER.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
    CURATOR_SERVER.deletePath(crawlId, XML_LABEL);
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
            .publisher(PUBLISHER)
            .curator(CURATOR_SERVER.getCurator())
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(EXECUTOR)
            .httpClient(HTTP_CLIENT)
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
    assertFalse(CURATOR_SERVER.checkExists(crawlId, XML_LABEL));
    assertFalse(CURATOR_SERVER.checkExists(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(XML_LABEL)));
    assertTrue(PUBLISHER.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
  }
}
