package org.gbif.pipelines.tasks.occurrences.indexing;

import static org.gbif.api.model.pipelines.PipelineStep.Status.COMPLETED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.FAILED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.PipelinesHistoryClientTestStub;
import org.gbif.pipelines.tasks.resources.EsServer;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IndexingCallbackIT {
  @ClassRule public static final EsServer ES_SERVER = EsServer.getInstance();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static ValidationWsClient validationClient;
  @Mock private static DatasetClient datasetClient;
  private static final CloseableHttpClient HTTP_CLIENT =
      HttpClients.custom()
          .setDefaultRequestConfig(
              RequestConfig.custom().setConnectTimeout(10_000).setSocketTimeout(10_000).build())
          .build();

  @After
  public void after() {
    PUBLISHER.close();
  }

  @Test
  public void successInterpretationTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    IndexingConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .httpClient(HTTP_CLIENT)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);

    // When
    callback.handleMessage(message);

    // Should
    Path path =
        Paths.get(
            config.stepConfig.repositoryPath
                + "/"
                + uuid
                + "/"
                + attempt
                + "/"
                + config.metaFileName);
    assertTrue(path.toFile().exists());
    assertEquals(1, PUBLISHER.getMessages().size());

    // Should
    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep indexingResult = result.get(StepType.INTERPRETED_TO_INDEX);
    Assert.assertNotNull(indexingResult);
    Assert.assertEquals(COMPLETED, indexingResult.getState());
  }

  @Test
  public void failedCaseTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 59; // Wrong attempt

    IndexingConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(PUBLISHER.getMessages().isEmpty());

    // Should
    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep indexingResult = result.get(StepType.INTERPRETED_TO_INDEX);
    Assert.assertNotNull(indexingResult);
    Assert.assertEquals(FAILED, indexingResult.getState());
  }

  @Test
  public void wrongRunnerCaseTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    IndexingConfiguration config = createConfig();
    config.processRunner = StepRunner.DISTRIBUTED.name(); // Message type is STANDALONE

    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(PUBLISHER.getMessages().isEmpty());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());
  }

  @Test
  public void failedDistrebutedCaseTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    IndexingConfiguration config = createConfig();
    config.processRunner = StepRunner.DISTRIBUTED.name();

    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setRunner(StepRunner.DISTRIBUTED.name());
    message.setPipelineSteps(Collections.singleton(StepType.INTERPRETED_TO_INDEX.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(PUBLISHER.getMessages().isEmpty());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep indexingResult = result.get(StepType.INTERPRETED_TO_INDEX);
    Assert.assertNotNull(indexingResult);
    Assert.assertEquals(FAILED, indexingResult.getState());
  }

  @Test
  public void wrongMessageSettingsCaseTest() {
    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();
    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    IndexingConfiguration config = createConfig();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    IndexingCallback callback =
        IndexingCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .executor(executor)
            .datasetClient(datasetClient)
            .build();

    PipelinesInterpretedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.HDFS_VIEW.name())); // Wrong type

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(PUBLISHER.getMessages().isEmpty());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());
  }

  private PipelinesInterpretedMessage createMessage(UUID uuid, int attempt) {
    PipelinesInterpretedMessage message = new PipelinesInterpretedMessage();
    message.setDatasetUuid(uuid);
    message.setAttempt(attempt);
    message.setEndpointType(EndpointType.DWC_ARCHIVE);
    message.setNumberOfRecords(1L);
    message.setRunner(StepRunner.STANDALONE.name());
    message.setInterpretTypes(Collections.singleton("ALL"));
    message.setValidationResult(new ValidationResult(true, true, false, 10L, 0L));
    message.setPipelineSteps(
        new HashSet<>(
            Arrays.asList(StepType.INTERPRETED_TO_INDEX.name(), StepType.HDFS_VIEW.name())));
    return message;
  }

  private IndexingConfiguration createConfig() {
    IndexingConfiguration config = new IndexingConfiguration();
    // Main
    config.standaloneNumberThreads = 1;
    config.processRunner = StepRunner.STANDALONE.name();
    // Indexing
    config.indexConfig.numberReplicas = 1;
    config.indexConfig.recordsPerShard = 1_000;
    config.indexConfig.bigIndexIfRecordsMoreThan = 10_000;
    config.indexConfig.defaultPrefixName = "default";
    config.indexConfig.defaultSize = 2_000;
    config.indexConfig.defaultNewIfSize = 2_500;
    config.indexConfig.defaultSmallestIndexCatUrl =
        ES_SERVER.getEsConfig().getRawHosts()[0]
            + "/_cat/indices/%s*?v&h=docs.count,index&s=docs.count:asc&format=json";
    config.indexConfig.occurrenceAlias = "occurrence";
    config.indexConfig.occurrenceVersion = "a";
    // ES
    config.esConfig.hosts = ES_SERVER.getEsConfig().getRawHosts();
    // Step config
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    config.pipelinesConfig = this.getClass().getClassLoader().getResource("lock.yaml").getPath();

    config.stepConfig.repositoryPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();
    return config;
  }
}
