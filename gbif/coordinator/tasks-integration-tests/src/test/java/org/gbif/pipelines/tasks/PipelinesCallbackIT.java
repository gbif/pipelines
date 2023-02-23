package org.gbif.pipelines.tasks;

import static org.gbif.api.model.pipelines.PipelineStep.Status.COMPLETED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.FAILED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.QUEUED;
import static org.gbif.api.model.pipelines.PipelineStep.Status.SUBMITTED;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.gbif.api.model.pipelines.PipelineExecution;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.registry.ws.client.DatasetClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PipelinesCallbackIT {
  @Mock private DatasetClient datasetClient;
  @Mock private MessagePublisher mockPublisher;

  @Test
  public void newFullIngestionCompletedTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();

    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    Long executionId = null; // New interpretation
    StepType stepType = StepType.DWCA_TO_VERBATIM;

    Set<String> pipelineSteps =
        new HashSet<>(
            Arrays.asList(
                StepType.DWCA_TO_VERBATIM.name(),
                StepType.VERBATIM_TO_IDENTIFIER.name(),
                StepType.VERBATIM_TO_INTERPRETED.name(),
                StepType.INTERPRETED_TO_INDEX.name(),
                StepType.HDFS_VIEW.name(),
                StepType.FRAGMENTER.name()));

    PipelineBasedMessage incomingMessage =
        TestMessage.create(
            datasetKey, attempt, pipelineSteps, executionId, DatasetType.OCCURRENCE, true, false);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .stepType(stepType)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create(executionId, DatasetType.OCCURRENCE, true, false))
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
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
  }

  @Test
  public void newHalfIngestionCompletedTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();

    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    Long executionId = null; // New interpretation
    StepType stepType = StepType.VERBATIM_TO_INTERPRETED;

    Set<String> pipelineSteps =
        new HashSet<>(
            Arrays.asList(
                StepType.VERBATIM_TO_INTERPRETED.name(),
                StepType.INTERPRETED_TO_INDEX.name(),
                StepType.HDFS_VIEW.name(),
                StepType.FRAGMENTER.name()));

    PipelineBasedMessage incomingMessage =
        TestMessage.create(
            datasetKey, attempt, pipelineSteps, executionId, DatasetType.OCCURRENCE, true, false);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .stepType(stepType)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create(executionId, DatasetType.OCCURRENCE, true, false))
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(4, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep interpretedResult = result.get(StepType.VERBATIM_TO_INTERPRETED);
    Assert.assertNotNull(interpretedResult);
    Assert.assertEquals(COMPLETED, interpretedResult.getState());

    PipelineStep indexingResult = result.get(StepType.INTERPRETED_TO_INDEX);
    Assert.assertNotNull(indexingResult);
    Assert.assertEquals(QUEUED, indexingResult.getState());

    PipelineStep fragmenterResult = result.get(StepType.FRAGMENTER);
    Assert.assertNotNull(fragmenterResult);
    Assert.assertEquals(QUEUED, fragmenterResult.getState());

    PipelineStep hdfsViewResult = result.get(StepType.HDFS_VIEW);
    Assert.assertNotNull(hdfsViewResult);
    Assert.assertEquals(QUEUED, hdfsViewResult.getState());
  }

  @Test
  public void newSingleStepIngestionCompletedTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();

    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    Long executionId = null; // New interpretation
    StepType stepType = StepType.INTERPRETED_TO_INDEX;

    Set<String> pipelineSteps = Collections.singleton(StepType.INTERPRETED_TO_INDEX.name());

    PipelineBasedMessage incomingMessage =
        TestMessage.create(
            datasetKey, attempt, pipelineSteps, executionId, DatasetType.OCCURRENCE, true, false);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .stepType(stepType)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create(executionId, DatasetType.OCCURRENCE, true, false))
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(1, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep indexingResult = result.get(StepType.INTERPRETED_TO_INDEX);
    Assert.assertNotNull(indexingResult);
    Assert.assertNotNull(indexingResult.getStarted());
    Assert.assertNotNull(indexingResult.getFinished());
    Assert.assertEquals(COMPLETED, indexingResult.getState());
  }

  @Test
  public void newFullIngestionFailedTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();

    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    Long executionId = null; // New interpretation
    StepType stepType = StepType.DWCA_TO_VERBATIM;

    Set<String> pipelineSteps =
        new HashSet<>(
            Arrays.asList(
                StepType.DWCA_TO_VERBATIM.name(),
                StepType.VERBATIM_TO_IDENTIFIER.name(),
                StepType.VERBATIM_TO_INTERPRETED.name(),
                StepType.INTERPRETED_TO_INDEX.name(),
                StepType.HDFS_VIEW.name(),
                StepType.FRAGMENTER.name()));

    PipelineBasedMessage incomingMessage =
        TestMessage.create(
            datasetKey, attempt, pipelineSteps, executionId, DatasetType.OCCURRENCE, true, false);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .stepType(stepType)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(new TestExceptionHandler(executionId, DatasetType.OCCURRENCE, true, false))
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
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
  public void twoStepsIngestionCompletedTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();

    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    Long executionId = null; // New interpretation

    Set<String> pipelineSteps =
        new HashSet<>(
            Arrays.asList(
                StepType.DWCA_TO_VERBATIM.name(),
                StepType.VERBATIM_TO_IDENTIFIER.name(),
                StepType.VERBATIM_TO_INTERPRETED.name(),
                StepType.INTERPRETED_TO_INDEX.name(),
                StepType.HDFS_VIEW.name(),
                StepType.FRAGMENTER.name()));

    PipelineBasedMessage incomingMessage =
        TestMessage.create(
            datasetKey, attempt, pipelineSteps, executionId, DatasetType.OCCURRENCE, true, false);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .stepType(StepType.DWCA_TO_VERBATIM)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create(executionId, DatasetType.OCCURRENCE, true, false))
        .config(TestConfig.create())
        .build()
        .handleMessage();

    long newExecutionId =
        historyClient.getPipelineExecutionMap().values().stream()
            .map(PipelineExecution::getKey)
            .findAny()
            .orElseThrow(() -> new PipelinesException("Can't fint execution"));

    incomingMessage.setExecutionId(newExecutionId);

    PipelinesCallback.builder()
        .message(incomingMessage)
        .stepType(StepType.VERBATIM_TO_IDENTIFIER)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create(newExecutionId, DatasetType.OCCURRENCE, true, false))
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(6, result.size());

    Assert.assertEquals(1, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(1, historyClient.getPipelineProcessMap().size());

    PipelineStep dwcaResult = result.get(StepType.DWCA_TO_VERBATIM);
    Assert.assertNotNull(dwcaResult);
    Assert.assertNotNull(dwcaResult.getFinished());
    Assert.assertEquals(COMPLETED, dwcaResult.getState());

    PipelineStep identifierResult = result.get(StepType.VERBATIM_TO_IDENTIFIER);
    Assert.assertNotNull(identifierResult);
    Assert.assertNotNull(identifierResult.getFinished());
    Assert.assertEquals(COMPLETED, identifierResult.getState());

    PipelineStep interpretedResult = result.get(StepType.VERBATIM_TO_INTERPRETED);
    Assert.assertNotNull(interpretedResult);
    Assert.assertNull(interpretedResult.getFinished());
    Assert.assertEquals(QUEUED, interpretedResult.getState());

    PipelineStep indexingResult = result.get(StepType.INTERPRETED_TO_INDEX);
    Assert.assertNotNull(indexingResult);
    Assert.assertNull(indexingResult.getFinished());
    Assert.assertEquals(SUBMITTED, indexingResult.getState());

    PipelineStep fragmenterResult = result.get(StepType.FRAGMENTER);
    Assert.assertNotNull(fragmenterResult);
    Assert.assertNull(fragmenterResult.getFinished());
    Assert.assertEquals(SUBMITTED, fragmenterResult.getState());

    PipelineStep hdfsViewResult = result.get(StepType.HDFS_VIEW);
    Assert.assertNotNull(hdfsViewResult);
    Assert.assertNull(hdfsViewResult.getFinished());
    Assert.assertEquals(SUBMITTED, hdfsViewResult.getState());
  }

  @Test(expected = NullPointerException.class)
  public void emptyBuilderTest() {
    // When
    PipelinesCallback.builder().build().handleMessage();
  }

  @Test
  public void getPipelinesVersionTest() {
    Assert.assertNotNull(PipelinesCallback.getPipelinesVersion());
  }

  @AllArgsConstructor(staticName = "create")
  private static class TestMessage implements PipelineBasedMessage {

    private final UUID datasetKey;
    private final Integer attempt;
    private final Set<String> pipelineSteps;
    private Long executionId;
    DatasetType datasetType;
    boolean containsOccurrences;
    boolean containsEvents;

    @Override
    public Integer getAttempt() {
      return attempt;
    }

    @Override
    public Set<String> getPipelineSteps() {
      return pipelineSteps;
    }

    @Override
    public Long getExecutionId() {
      return executionId;
    }

    @Override
    public void setExecutionId(Long executionId) {
      this.executionId = executionId;
    }

    @Override
    public DatasetInfo getDatasetInfo() {
      return new DatasetInfo(datasetType, containsOccurrences, containsEvents);
    }

    @Override
    public UUID getDatasetUuid() {
      return datasetKey;
    }

    @Override
    public String getRoutingKey() {
      return "";
    }
  }

  private static class TestExceptionHandler extends TestHandler {

    private TestExceptionHandler(
        Long executionId,
        DatasetType datasetType,
        boolean containsOccurrences,
        boolean containsEvents) {
      super(executionId, datasetType, containsOccurrences, containsEvents);
    }

    @Override
    public Runnable createRunnable(PipelineBasedMessage message) {
      return () -> {
        throw new IllegalStateException("Oops!");
      };
    }
  }

  @AllArgsConstructor(staticName = "create")
  private static class TestHandler
      implements StepHandler<PipelineBasedMessage, PipelineBasedMessage> {

    private Long executionId;
    private DatasetType datasetType;
    private boolean containsOccurrences;
    private boolean containsEvents;

    @Override
    public Runnable createRunnable(PipelineBasedMessage message) {
      return () -> {};
    }

    @Override
    public PipelineBasedMessage createOutgoingMessage(PipelineBasedMessage message) {
      UUID datasetKey = message.getDatasetUuid();
      int attempt = message.getAttempt();
      Set<String> pipelineSteps = message.getPipelineSteps();
      return TestMessage.create(
          datasetKey,
          attempt,
          pipelineSteps,
          executionId,
          datasetType,
          containsOccurrences,
          containsEvents);
    }

    @Override
    public boolean isMessageCorrect(PipelineBasedMessage message) {
      return true;
    }
  }

  @AllArgsConstructor(staticName = "create")
  private static class TestConfig implements BaseConfiguration {

    @Override
    public String getHdfsSiteConfig() {
      return "";
    }

    @Override
    public String getCoreSiteConfig() {
      return "";
    }

    @Override
    public String getRepositoryPath() {
      return "";
    }

    @Override
    public String getMetaFileName() {
      return "";
    }
  }
}
