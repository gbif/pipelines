package org.gbif.pipelines.tasks.verbatims.dwca;

import static org.gbif.api.model.pipelines.PipelineStep.Status.COMPLETED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.FAILED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.QUEUED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.SUBMITTED;
import static org.gbif.api.model.pipelines.StepType.DWCA_TO_VERBATIM;
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
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.Platform;
import org.gbif.pipelines.common.utils.HdfsUtils;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.PipelinesHistoryClientTestStub;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test archive-to-avro commands message handling command on hdfs */
@RunWith(MockitoJUnitRunner.class)
public class DwcaToAvroCallbackIT {
  private static final String DATASET_UUID = "35d24686-95c7-43f2-969f-611bba488512";
  private static final String DUMMY_URL = "http://some.new.url";
  private static final String INPUT_DATASET_FOLDER = "/dataset/dwca";
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static ValidationWsClient validationClient;
  @Mock private static DatasetClient datasetClient;

  @After
  public void after() {
    PUBLISHER.close();
  }

  @Test
  public void normalCaseTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        DwcaToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;

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
    assertTrue(path.toFile().exists());
    assertTrue(Files.size(path) > 0L);
    assertEquals(1, PUBLISHER.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep dwcaResult = result.get(StepType.DWCA_TO_VERBATIM);
    Assert.assertNotNull(dwcaResult);
    Assert.assertEquals(COMPLETED, dwcaResult.getState());

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
  public void csvCaseTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource("/dataset/csv").getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        DwcaToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString("189136b2-3d94-4cc6-bd86-42c85b27cbb4");
    int attempt = 2;

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
    Path path = Paths.get(config.stepConfig.repositoryPath + uuid + "/2/verbatim.avro");
    assertTrue(path.toFile().exists());
    assertTrue(Files.size(path) > 0L);
    assertEquals(1, PUBLISHER.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep dwcaResult = result.get(StepType.DWCA_TO_VERBATIM);
    Assert.assertNotNull(dwcaResult);
    Assert.assertEquals(COMPLETED, dwcaResult.getState());

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
  public void xlsxCaseTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource("/dataset/xlsx").getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        DwcaToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString("b0494b4a-b9fb-49d5-9f55-869ad5d13ae9");
    int attempt = 2;

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
    Path path = Paths.get(config.stepConfig.repositoryPath + uuid + "/2/verbatim.avro");
    assertTrue(path.toFile().exists());
    assertTrue(Files.size(path) > 0L);
    assertEquals(1, PUBLISHER.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep dwcaResult = result.get(StepType.DWCA_TO_VERBATIM);
    Assert.assertNotNull(dwcaResult);
    Assert.assertEquals(COMPLETED, dwcaResult.getState());

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

  @Ignore
  @Test
  public void odsCaseTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource("/dataset/ods").getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        DwcaToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString("15d05310-3fcf-4cde-b210-9b398a24c846");
    int attempt = 2;

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
    Path path = Paths.get(config.stepConfig.repositoryPath + uuid + "/2/verbatim.avro");
    assertTrue(path.toFile().exists());
    assertTrue(Files.size(path) > 0L);
    assertEquals(1, PUBLISHER.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep dwcaResult = result.get(StepType.DWCA_TO_VERBATIM);
    Assert.assertNotNull(dwcaResult);
    Assert.assertEquals(COMPLETED, dwcaResult.getState());

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
  public void normalSingleStepCaseTest() throws Exception {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        DwcaToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;

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
            null);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + DATASET_UUID + "/2/verbatim.avro");
    assertTrue(path.toFile().exists());
    assertTrue(Files.size(path) > 0L);
    assertEquals(1, PUBLISHER.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep dwcaResult = result.get(StepType.DWCA_TO_VERBATIM);
    Assert.assertNotNull(dwcaResult);
    Assert.assertEquals(COMPLETED, dwcaResult.getState());

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
  public void failedCaseTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile() + "/1";
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        DwcaToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;

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
    assertFalse(path.toFile().exists());
    assertTrue(PUBLISHER.getMessages().isEmpty());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep dwcaResult = result.get(StepType.DWCA_TO_VERBATIM);
    Assert.assertNotNull(dwcaResult);
    Assert.assertEquals(FAILED, dwcaResult.getState());

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
  }

  @Test
  public void invalidReportStatusTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    DwcaToAvroConfiguration config = new DwcaToAvroConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    DwcaToAvroCallback callback =
        DwcaToAvroCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;

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
            null);

    // When
    callback.handleMessage(message);

    // Should
    Path path = Paths.get(config.stepConfig.repositoryPath + DATASET_UUID + "/2/verbatim.avro");
    assertFalse(path.toFile().exists());
    assertTrue(PUBLISHER.getMessages().isEmpty());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());
  }
}
