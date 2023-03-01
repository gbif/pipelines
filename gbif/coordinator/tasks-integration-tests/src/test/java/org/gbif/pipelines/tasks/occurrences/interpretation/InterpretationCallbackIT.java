package org.gbif.pipelines.tasks.occurrences.interpretation;

import static org.gbif.api.model.pipelines.PipelineStep.Status.FAILED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.SUBMITTED;
import static org.gbif.api.model.pipelines.StepRunner.DISTRIBUTED;
import static org.gbif.api.model.pipelines.StepRunner.STANDALONE;
import static org.gbif.api.model.pipelines.StepType.VERBATIM_TO_INTERPRETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.tasks.CloseableHttpClientStub;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.PipelinesHistoryClientTestStub;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InterpretationCallbackIT {
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static DatasetClient datasetClient;
  @Mock private static ValidationWsClient validationClient;
  @Mock private static CloseableHttpClient httpClient;

  @After
  public void after() {
    PUBLISHER.close();
  }

  @Test
  public void invalidMessageRunnerTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    InterpreterConfiguration config = new InterpreterConfiguration();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/occurrence/").getFile();
    config.processRunner = STANDALONE.name();
    config.pipelinesConfig = "pipelines.yaml";

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    InterpretationCallback callback =
        InterpretationCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .httpClient(httpClient)
            .executor(executorService)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    ValidationResult validationResult = new ValidationResult(true, true, false, 10L, null);

    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            uuid,
            attempt,
            Collections.singleton(RecordType.ALL.name()),
            Collections.singleton(VERBATIM_TO_INTERPRETED.name()),
            DISTRIBUTED.name(),
            EndpointType.DWC_ARCHIVE,
            null,
            validationResult,
            null,
            null,
            null);

    // When
    callback.handleMessage(message);

    // Should
    Path path =
        Paths.get(config.stepConfig.repositoryPath + DATASET_UUID + "/" + attempt + "/interpreted");
    assertFalse(path.toFile().exists());

    assertEquals(0, PUBLISHER.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());
  }

  @Test
  public void invalidChildSystemProcessTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    InterpreterConfiguration config = new InterpreterConfiguration();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/occurrence/").getFile();
    config.processRunner = DISTRIBUTED.name();
    config.pipelinesConfig = "pipelines.yaml";
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";

    config.sparkConfig.recordsPerThread = 100000;
    config.sparkConfig.parallelismMin = 10;
    config.sparkConfig.parallelismMax = 100;
    config.sparkConfig.memoryOverhead = 1280;
    config.sparkConfig.executorMemoryGbMin = 4;
    config.sparkConfig.executorMemoryGbMax = 12;
    config.sparkConfig.executorCores = 5;
    config.sparkConfig.executorNumbersMin = 6;
    config.sparkConfig.executorNumbersMax = 10;
    config.sparkConfig.driverMemory = "1G";

    config.distributedConfig.deployMode = "cluster";
    config.distributedConfig.mainClass =
        "org.gbif.pipelines.ingest.pipelines.VerbatimToInterpretedPipeline";
    config.distributedConfig.jarPath = "a://b/a/c/ingest-gbif.jar";

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    CloseableHttpClient closeableHttpClient = new CloseableHttpClientStub(200, "[]");

    InterpretationCallback callback =
        InterpretationCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .httpClient(closeableHttpClient)
            .executor(executorService)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    ValidationResult validationResult = new ValidationResult(true, true, false, 100L, null);

    PipelinesVerbatimMessage message =
        new PipelinesVerbatimMessage(
            uuid,
            attempt,
            Collections.singleton(RecordType.ALL.name()),
            Collections.singleton(VERBATIM_TO_INTERPRETED.name()),
            DISTRIBUTED.name(),
            EndpointType.DWC_ARCHIVE,
            null,
            validationResult,
            null,
            null,
            null);

    // When
    callback.handleMessage(message);

    // Should
    assertEquals(0, PUBLISHER.getMessages().size());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(4, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep interpretedResult = result.get(StepType.VERBATIM_TO_INTERPRETED);
    Assert.assertNotNull(interpretedResult);
    Assert.assertEquals(FAILED, interpretedResult.getState());

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
}
