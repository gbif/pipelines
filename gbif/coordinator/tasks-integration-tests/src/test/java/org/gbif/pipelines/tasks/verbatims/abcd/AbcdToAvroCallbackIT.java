package org.gbif.pipelines.tasks.verbatims.abcd;

import static org.gbif.api.model.pipelines.StepType.ABCD_TO_VERBATIM;
import static org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.resources.CuratorServer;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroCallback;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroConfiguration;
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
public class AbcdToAvroCallbackIT {

  @ClassRule public static final CuratorServer CURATOR_SERVER = CuratorServer.getInstance();
  private static final String AVRO = "/verbatim.avro";
  private static final String STRING_UUID = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
  private static final UUID DATASET_UUID = UUID.fromString(STRING_UUID);
  private static final String INPUT_DATASET_FOLDER = "/dataset";
  private static final long EXECUTION_ID = 1L;
  private static final String ABCD_LABEL = ABCD_TO_VERBATIM.getLabel();
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

  @Mock private static PipelinesHistoryClient historyClient;
  @Mock private static ValidationWsClient validationClient;
  @Mock private static DatasetClient datasetClient;
  @Mock private static CloseableHttpClient httpClient;

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
    int attempt = 60;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "abcd";

    AbcdToAvroCallback callback =
        AbcdToAvroCallback.builder()
            .curator(CURATOR_SERVER.getCurator())
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .callback(
                XmlToAvroCallback.builder()
                    .config(config)
                    .publisher(PUBLISHER)
                    .curator(CURATOR_SERVER.getCurator())
                    .historyClient(historyClient)
                    .validationClient(validationClient)
                    .executor(EXECUTOR)
                    .httpClient(httpClient)
                    .datasetClient(datasetClient)
                    .build())
            .build();

    PipelinesAbcdMessage message =
        new PipelinesAbcdMessage(
            DATASET_UUID,
            URI.create("https://www.gbif.org/"),
            attempt,
            true,
            Collections.emptySet(),
            EndpointType.BIOCASE_XML_ARCHIVE,
            EXECUTION_ID);
    String crawlId = DATASET_UUID.toString();

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + STRING_UUID + "/" + attempt + AVRO);
    assertTrue(path.toFile().exists());
    assertTrue(Files.size(path) > 0L);
    assertTrue(CURATOR_SERVER.checkExists(crawlId, ABCD_LABEL));
    assertTrue(CURATOR_SERVER.checkExists(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(ABCD_LABEL)));
    assertEquals(1, PUBLISHER.getMessages().size());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
    CURATOR_SERVER.deletePath(crawlId, ABCD_LABEL);
  }

  @Test
  public void testFailedCaseWhenXmlAvroEmpty() {
    // State
    String datasetKey = "182778bd-579e-4ba7-baef-921c3b9db9a0";
    int attempt = 62;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "abcd";

    AbcdToAvroCallback callback =
        AbcdToAvroCallback.builder()
            .curator(CURATOR_SERVER.getCurator())
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .callback(
                XmlToAvroCallback.builder()
                    .config(config)
                    .publisher(PUBLISHER)
                    .curator(CURATOR_SERVER.getCurator())
                    .historyClient(historyClient)
                    .validationClient(validationClient)
                    .executor(EXECUTOR)
                    .httpClient(httpClient)
                    .datasetClient(datasetClient)
                    .build())
            .build();

    PipelinesAbcdMessage message =
        new PipelinesAbcdMessage(
            UUID.fromString(datasetKey),
            URI.create("https://www.gbif.org/"),
            attempt,
            true,
            Collections.emptySet(),
            EndpointType.DIGIR,
            EXECUTION_ID);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + datasetKey + "/" + attempt + AVRO);
    assertFalse(path.toFile().exists());
    assertTrue(path.getParent().toFile().exists());
    assertTrue(CURATOR_SERVER.checkExists(datasetKey, ABCD_LABEL));
    assertTrue(CURATOR_SERVER.checkExists(datasetKey, Fn.ERROR_MESSAGE.apply(ABCD_LABEL)));
    assertTrue(PUBLISHER.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
    CURATOR_SERVER.deletePath(datasetKey, ABCD_LABEL);
  }

  @Test
  public void testReasonFailCase() {
    // State
    int attempt = 60;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "abcd";

    AbcdToAvroCallback callback =
        AbcdToAvroCallback.builder()
            .curator(CURATOR_SERVER.getCurator())
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .callback(
                XmlToAvroCallback.builder()
                    .config(config)
                    .publisher(PUBLISHER)
                    .curator(CURATOR_SERVER.getCurator())
                    .historyClient(historyClient)
                    .validationClient(validationClient)
                    .executor(EXECUTOR)
                    .httpClient(httpClient)
                    .datasetClient(datasetClient)
                    .build())
            .build();

    PipelinesAbcdMessage message =
        new PipelinesAbcdMessage(
            DATASET_UUID,
            URI.create("https://www.gbif.org/"),
            attempt,
            false,
            Collections.emptySet(),
            EndpointType.DIGIR,
            EXECUTION_ID);
    String crawlId = DATASET_UUID.toString();

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + STRING_UUID + "/" + attempt + AVRO);
    assertFalse(path.toFile().exists());
    assertFalse(CURATOR_SERVER.checkExists(crawlId, ABCD_LABEL));
    assertFalse(CURATOR_SERVER.checkExists(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(ABCD_LABEL)));
    assertTrue(PUBLISHER.getMessages().isEmpty());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
  }
}
