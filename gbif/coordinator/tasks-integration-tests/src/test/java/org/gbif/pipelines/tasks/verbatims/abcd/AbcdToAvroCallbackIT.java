package org.gbif.pipelines.tasks.verbatims.abcd;

import static org.gbif.api.model.pipelines.PipelineStep.Status.COMPLETED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.FAILED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.QUEUED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.SUBMITTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesAbcdMessage;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.PipelinesHistoryClientTestStub;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroCallback;
import org.gbif.pipelines.tasks.verbatims.xml.XmlToAvroConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AbcdToAvroCallbackIT {

  private static final String AVRO = "/verbatim.avro";
  private static final String STRING_UUID = "7ef15372-1387-11e2-bb2e-00145eb45e9a";
  private static final UUID DATASET_UUID = UUID.fromString(STRING_UUID);
  private static final String INPUT_DATASET_FOLDER = "/dataset";
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();

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
  public void normalCaseWhenFilesInDirectoryTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    int attempt = 60;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "abcd";

    AbcdToAvroCallback callback =
        AbcdToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .callback(
                XmlToAvroCallback.builder()
                    .config(config)
                    .publisher(PUBLISHER)
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
            null);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + STRING_UUID + "/" + attempt + AVRO);
    assertTrue(path.toFile().exists());
    assertTrue(Files.size(path) > 0L);
    assertEquals(1, PUBLISHER.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep abcdResult = result.get(StepType.ABCD_TO_VERBATIM);
    Assert.assertNotNull(abcdResult);
    Assert.assertEquals(COMPLETED, abcdResult.getState());

    PipelineStep identifierResult = result.get(StepType.VERBATIM_TO_IDENTIFIER);
    Assert.assertNotNull(identifierResult);
    Assert.assertEquals(QUEUED, identifierResult.getState());

    PipelineStep interpretedResult = result.get(StepType.VERBATIM_TO_INTERPRETED);
    Assert.assertNotNull(interpretedResult);
    Assert.assertEquals(SUBMITTED, interpretedResult.getState());

    PipelineStep indexingResult = result.get(StepType.INTERPRETED_TO_INDEX);
    Assert.assertNotNull(indexingResult);
    Assert.assertEquals(SUBMITTED, indexingResult.getState());

    PipelineStep fragmenterResult = result.get(StepType.FRAGMENTER);
    Assert.assertNotNull(fragmenterResult);
    Assert.assertEquals(SUBMITTED, fragmenterResult.getState());

    PipelineStep hdfsViewResult = result.get(StepType.HDFS_VIEW);
    Assert.assertNotNull(hdfsViewResult);
    Assert.assertEquals(SUBMITTED, hdfsViewResult.getState());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
  }

  @Test
  public void failedCaseWhenXmlAvroEmptyTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    String datasetKey = "182778bd-579e-4ba7-baef-921c3b9db9a0";
    int attempt = 62;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "abcd";

    AbcdToAvroCallback callback =
        AbcdToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .callback(
                XmlToAvroCallback.builder()
                    .config(config)
                    .publisher(PUBLISHER)
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
            null);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + datasetKey + "/" + attempt + AVRO);
    assertFalse(path.toFile().exists());
    assertTrue(path.getParent().toFile().exists());
    assertTrue(PUBLISHER.getMessages().isEmpty());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep abcdResult = result.get(StepType.ABCD_TO_VERBATIM);
    Assert.assertNotNull(abcdResult);
    Assert.assertEquals(FAILED, abcdResult.getState());

    PipelineStep identifierResult = result.get(StepType.VERBATIM_TO_IDENTIFIER);
    Assert.assertNotNull(identifierResult);
    Assert.assertEquals(SUBMITTED, identifierResult.getState());

    PipelineStep interpretedResult = result.get(StepType.VERBATIM_TO_INTERPRETED);
    Assert.assertNotNull(interpretedResult);
    Assert.assertEquals(SUBMITTED, interpretedResult.getState());

    PipelineStep indexingResult = result.get(StepType.INTERPRETED_TO_INDEX);
    Assert.assertNotNull(indexingResult);
    Assert.assertEquals(SUBMITTED, indexingResult.getState());

    PipelineStep fragmenterResult = result.get(StepType.FRAGMENTER);
    Assert.assertNotNull(fragmenterResult);
    Assert.assertEquals(SUBMITTED, fragmenterResult.getState());

    PipelineStep hdfsViewResult = result.get(StepType.HDFS_VIEW);
    Assert.assertNotNull(hdfsViewResult);
    Assert.assertEquals(SUBMITTED, hdfsViewResult.getState());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
  }

  @Test
  public void reasonFailCaseTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    int attempt = 60;
    XmlToAvroConfiguration config = new XmlToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();
    config.xmlReaderParallelism = 4;
    config.archiveRepositorySubdir = "abcd";

    AbcdToAvroCallback callback =
        AbcdToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .callback(
                XmlToAvroCallback.builder()
                    .config(config)
                    .publisher(PUBLISHER)
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
            null);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + STRING_UUID + "/" + attempt + AVRO);
    assertFalse(path.toFile().exists());
    assertTrue(PUBLISHER.getMessages().isEmpty());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());

    // Clean
    HdfsUtils.deleteDirectory(HdfsConfigs.nullConfig(), path.toString());
  }
}
