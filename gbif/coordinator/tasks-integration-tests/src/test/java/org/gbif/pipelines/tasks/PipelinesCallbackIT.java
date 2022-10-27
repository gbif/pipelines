package org.gbif.pipelines.tasks;

import static org.gbif.crawler.constants.CrawlerNodePaths.PROCESS_STATE_OCCURRENCE;
import static org.gbif.crawler.constants.PipelinesNodePaths.SIZE;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.zookeeper.CreateMode;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.crawler.constants.CrawlerNodePaths;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.configs.BaseConfiguration;
import org.gbif.pipelines.tasks.resources.CuratorServer;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PipelinesCallbackIT {

  @ClassRule public static final CuratorServer CURATOR_SERVER = CuratorServer.getInstance();
  private static final Long EXECUTION_ID = 1L;
  @Mock private DatasetClient datasetClient;
  @Mock private PipelinesHistoryClient historyClient;
  @Mock private MessagePublisher mockPublisher;

  @Test(expected = NullPointerException.class)
  public void testEmptyBuilder() {
    // When
    PipelinesCallback.builder().build().handleMessage();
  }

  @Test
  public void testNullMessagePublisher() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey.toString();
    String rootPath = StepType.DWCA_TO_VERBATIM.getLabel();
    StepType nextStepName = StepType.DWCA_TO_VERBATIM;
    Set<String> pipelineSteps =
        new HashSet<>(
            Arrays.asList(
                StepType.DWCA_TO_VERBATIM.name(),
                StepType.VERBATIM_TO_INTERPRETED.name(),
                StepType.INTERPRETED_TO_INDEX.name(),
                StepType.HDFS_VIEW.name()));
    PipelineBasedMessage incomingMessage = TestMessage.create(datasetKey, attempt, pipelineSteps);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .curator(CURATOR_SERVER.getCurator())
        .stepType(nextStepName)
        .publisher(null)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create())
        .config(TestConfig.create())
        .build()
        .handleMessage();

    Optional<LocalDateTime> startDate = getAsDate(crawlId, Fn.START_DATE.apply(rootPath));
    Optional<LocalDateTime> endDate = getAsDate(crawlId, Fn.END_DATE.apply(rootPath));

    // Run second time to check
    PipelinesCallback.builder()
        .message(incomingMessage)
        .curator(CURATOR_SERVER.getCurator())
        .stepType(nextStepName)
        .publisher(null)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create())
        .config(TestConfig.create())
        .build()
        .handleMessage();

    Optional<LocalDateTime> startDateTwo = getAsDate(crawlId, Fn.START_DATE.apply(rootPath));
    Optional<LocalDateTime> endDateTwo = getAsDate(crawlId, Fn.END_DATE.apply(rootPath));

    // Should
    Assert.assertTrue(startDate.isPresent());
    Assert.assertTrue(endDate.isPresent());

    Assert.assertTrue(startDateTwo.isPresent());
    Assert.assertTrue(endDateTwo.isPresent());

    Assert.assertEquals(startDate.get(), startDateTwo.get());
    Assert.assertEquals(endDate.get(), endDateTwo.get());

    Assert.assertTrue(getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(rootPath)).isPresent());
    Assert.assertTrue(getAsString(crawlId, Fn.ERROR_MESSAGE.apply(rootPath)).isPresent());

    Assert.assertTrue(
        getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(rootPath)).isPresent());
    Assert.assertFalse(getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(rootPath)).isPresent());

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testBaseHandlerBehavior() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey.toString();
    String rootPath = StepType.DWCA_TO_VERBATIM.getLabel();
    StepType nextStepName = StepType.DWCA_TO_VERBATIM;
    Set<String> pipelineSteps =
        new HashSet<>(
            Arrays.asList(
                StepType.DWCA_TO_VERBATIM.name(),
                StepType.VERBATIM_TO_INTERPRETED.name(),
                StepType.INTERPRETED_TO_INDEX.name(),
                StepType.HDFS_VIEW.name()));
    PipelineBasedMessage incomingMessage = TestMessage.create(datasetKey, attempt, pipelineSteps);

    updateMonitoring(crawlId, SIZE, String.valueOf(2));

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .curator(CURATOR_SERVER.getCurator())
        .stepType(nextStepName)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create())
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
    Assert.assertTrue(getAsDate(crawlId, Fn.START_DATE.apply(rootPath)).isPresent());
    Assert.assertTrue(getAsDate(crawlId, Fn.END_DATE.apply(rootPath)).isPresent());

    Assert.assertFalse(getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(rootPath)).isPresent());
    Assert.assertFalse(getAsString(crawlId, Fn.ERROR_MESSAGE.apply(rootPath)).isPresent());

    Assert.assertTrue(
        getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(rootPath)).isPresent());
    Assert.assertTrue(getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(rootPath)).isPresent());

    Assert.assertTrue(getAsString(crawlId, SIZE).isPresent());
    Assert.assertEquals("1", getAsString(crawlId, SIZE).get());

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testOneStepHandler() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey.toString();
    StepType nextStepName = StepType.DWCA_TO_VERBATIM;
    Set<String> pipelineSteps = Collections.singleton(StepType.DWCA_TO_VERBATIM.name());
    PipelineBasedMessage incomingMessage = TestMessage.create(datasetKey, attempt, pipelineSteps);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .curator(CURATOR_SERVER.getCurator())
        .stepType(nextStepName)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create())
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
    Assert.assertFalse(checkExists(getPipelinesInfoPath(crawlId)));

    String crawlInfoPath = CrawlerNodePaths.getCrawlInfoPath(datasetKey, PROCESS_STATE_OCCURRENCE);
    Assert.assertFalse(checkExists(crawlInfoPath));

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testRunnerException() throws Exception {

    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey.toString();
    String rootPath = StepType.DWCA_TO_VERBATIM.getLabel();
    StepType nextStepName = StepType.DWCA_TO_VERBATIM;
    Set<String> pipelineSteps =
        new HashSet<>(
            Arrays.asList(
                StepType.DWCA_TO_VERBATIM.name(),
                StepType.VERBATIM_TO_INTERPRETED.name(),
                StepType.INTERPRETED_TO_INDEX.name(),
                StepType.HDFS_VIEW.name()));
    PipelineBasedMessage incomingMessage = TestMessage.create(datasetKey, attempt, pipelineSteps);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .curator(CURATOR_SERVER.getCurator())
        .stepType(nextStepName)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestExceptionHandler.create())
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
    Assert.assertTrue(getAsDate(crawlId, Fn.START_DATE.apply(rootPath)).isPresent());
    Assert.assertFalse(getAsDate(crawlId, Fn.END_DATE.apply(rootPath)).isPresent());

    Assert.assertTrue(getAsBoolean(crawlId, Fn.ERROR_AVAILABILITY.apply(rootPath)).isPresent());
    Assert.assertTrue(getAsString(crawlId, Fn.ERROR_MESSAGE.apply(rootPath)).isPresent());

    Assert.assertFalse(
        getAsBoolean(crawlId, Fn.SUCCESSFUL_AVAILABILITY.apply(rootPath)).isPresent());
    Assert.assertFalse(getAsString(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(rootPath)).isPresent());

    Assert.assertFalse(getAsString(crawlId, SIZE).isPresent());

    String crawlInfoPath = CrawlerNodePaths.getCrawlInfoPath(datasetKey, PROCESS_STATE_OCCURRENCE);
    Assert.assertTrue(checkExists(crawlInfoPath));
    Assert.assertTrue(getAsString(crawlInfoPath).isPresent());
    Assert.assertEquals("FINISHED", getAsString(crawlInfoPath).get());

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testLastPipelineStep() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey.toString();
    StepType nextStepName = StepType.DWCA_TO_VERBATIM;
    Set<String> pipelineSteps = Collections.singleton(StepType.DWCA_TO_VERBATIM.name());
    PipelineBasedMessage incomingMessage = TestMessage.create(datasetKey, attempt, pipelineSteps);
    updateMonitoring(crawlId, SIZE, String.valueOf(1));

    String crawlInfoPath = CrawlerNodePaths.getCrawlInfoPath(datasetKey, PROCESS_STATE_OCCURRENCE);
    updateMonitoring(crawlInfoPath, "RUNNING");

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .curator(CURATOR_SERVER.getCurator())
        .stepType(nextStepName)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create())
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
    Assert.assertFalse(checkExists(getPipelinesInfoPath(crawlId)));

    Assert.assertTrue(checkExists(crawlInfoPath));
    Assert.assertTrue(getAsString(crawlInfoPath).isPresent());
    Assert.assertEquals("FINISHED", getAsString(crawlInfoPath).get());

    // Postprocess
    deleteMonitoringById(crawlId);
  }

  @Test
  public void testMonitoringStepCounter() throws Exception {

    // State
    UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
    int attempt = 1;
    String crawlId = datasetKey.toString();
    StepType nextStepName = StepType.DWCA_TO_VERBATIM;
    Set<String> pipelineSteps =
        new HashSet<>(
            Arrays.asList(
                StepType.DWCA_TO_VERBATIM.name(),
                StepType.VERBATIM_TO_INTERPRETED.name(),
                StepType.INTERPRETED_TO_INDEX.name(),
                StepType.HDFS_VIEW.name()));
    PipelineBasedMessage incomingMessage = TestMessage.create(datasetKey, attempt, pipelineSteps);

    // When
    PipelinesCallback.builder()
        .message(incomingMessage)
        .curator(CURATOR_SERVER.getCurator())
        .stepType(nextStepName)
        .publisher(mockPublisher)
        .historyClient(historyClient)
        .datasetClient(datasetClient)
        .handler(TestHandler.create())
        .config(TestConfig.create())
        .build()
        .handleMessage();

    // Should
    Assert.assertTrue(checkExists(getPipelinesInfoPath(crawlId)));
    Assert.assertTrue(getAsString(crawlId, SIZE).isPresent());
    Assert.assertEquals("3", getAsString(crawlId, SIZE).get());

    String crawlInfoPath = CrawlerNodePaths.getCrawlInfoPath(datasetKey, PROCESS_STATE_OCCURRENCE);
    Assert.assertTrue(checkExists(crawlInfoPath));
    Assert.assertTrue(getAsString(crawlInfoPath).isPresent());
    Assert.assertEquals("RUNNING", getAsString(crawlInfoPath).get());

    // Postprocess
    deleteMonitoringById(crawlId);
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
      return EXECUTION_ID;
    }

    @Override
    public void setExecutionId(Long executionId) {
      // do nothing
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

  /**
   * Check exists a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private boolean checkExists(String crawlId) throws Exception {
    return CURATOR_SERVER.getCurator().checkExists().forPath(crawlId) != null;
  }

  /**
   * Removes a Zookeeper monitoring root node by crawlId
   *
   * @param crawlId root node path
   */
  private void deleteMonitoringById(String crawlId) throws Exception {
    String path = getPipelinesInfoPath(crawlId);
    if (checkExists(path)) {
      CURATOR_SERVER.getCurator().delete().deletingChildrenIfNeeded().forPath(path);
    }
  }

  /**
   * Creates or updates a String value for a Zookeeper monitoring node
   *
   * @param crawlId root node path
   * @param path child node path
   * @param value some String value
   */
  private void updateMonitoring(String crawlId, String path, String value) throws Exception {
    String fullPath = getPipelinesInfoPath(crawlId, path);
    updateMonitoring(fullPath, value);
  }

  /**
   * Creates or updates a String value for a Zookeeper monitoring node
   *
   * @param fullPath node path
   * @param value some String value
   */
  private void updateMonitoring(String fullPath, String value) throws Exception {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (checkExists(fullPath)) {
      CURATOR_SERVER.getCurator().setData().forPath(fullPath, bytes);
    } else {
      CURATOR_SERVER
          .getCurator()
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(fullPath, bytes);
    }
  }

  /** Read value from Zookeeper as a {@link String} */
  private Optional<String> getAsString(String crawlId, String path) throws Exception {
    String infoPath = getPipelinesInfoPath(crawlId, path);
    return getAsString(infoPath);
  }

  /** Read value from Zookeeper as a {@link String} */
  private Optional<String> getAsString(String infoPath) throws Exception {
    if (CURATOR_SERVER.getCurator().checkExists().forPath(infoPath) != null) {
      byte[] responseData = CURATOR_SERVER.getCurator().getData().forPath(infoPath);
      if (responseData != null) {
        return Optional.of(new String(responseData, StandardCharsets.UTF_8));
      }
    }
    return Optional.empty();
  }

  /** Read value from Zookeeper as a {@link LocalDateTime} */
  private Optional<LocalDateTime> getAsDate(String crawlId, String path) throws Exception {
    Optional<String> data = getAsString(crawlId, path);
    try {
      return data.map(x -> LocalDateTime.parse(x, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    } catch (DateTimeParseException ex) {
      return Optional.empty();
    }
  }

  /** Read value from Zookeeper as a {@link Boolean} */
  private Optional<Boolean> getAsBoolean(String crawlId, String path) throws Exception {
    Optional<String> data = getAsString(crawlId, path);
    try {
      return data.map(Boolean::parseBoolean);
    } catch (DateTimeParseException ex) {
      return Optional.empty();
    }
  }

  @NoArgsConstructor(staticName = "create")
  private static class TestExceptionHandler
      implements StepHandler<PipelineBasedMessage, PipelineBasedMessage> {

    @Override
    public Runnable createRunnable(PipelineBasedMessage message) {
      return () -> {
        throw new IllegalStateException("Oops!");
      };
    }

    @Override
    public PipelineBasedMessage createOutgoingMessage(PipelineBasedMessage message) {
      UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
      int attempt = 1;
      Set<String> pipelineSteps =
          new HashSet<>(
              Arrays.asList(
                  StepType.DWCA_TO_VERBATIM.name(),
                  StepType.VERBATIM_TO_INTERPRETED.name(),
                  StepType.INTERPRETED_TO_INDEX.name(),
                  StepType.HDFS_VIEW.name()));
      return TestMessage.create(datasetKey, attempt, pipelineSteps);
    }

    @Override
    public boolean isMessageCorrect(PipelineBasedMessage message) {
      return true;
    }
  }

  @NoArgsConstructor(staticName = "create")
  private static class TestHandler
      implements StepHandler<PipelineBasedMessage, PipelineBasedMessage> {

    @Override
    public Runnable createRunnable(PipelineBasedMessage message) {
      return () -> {};
    }

    @Override
    public PipelineBasedMessage createOutgoingMessage(PipelineBasedMessage message) {
      UUID datasetKey = UUID.fromString("a731e3b1-bc81-4c1f-aad7-aba75ce3cf3b");
      int attempt = 1;
      Set<String> pipelineSteps =
          new HashSet<>(
              Arrays.asList(
                  StepType.DWCA_TO_VERBATIM.name(),
                  StepType.VERBATIM_TO_INTERPRETED.name(),
                  StepType.INTERPRETED_TO_INDEX.name(),
                  StepType.HDFS_VIEW.name()));
      return TestMessage.create(datasetKey, attempt, pipelineSteps);
    }

    @Override
    public boolean isMessageCorrect(PipelineBasedMessage message) {
      return true;
    }
  }

  @NoArgsConstructor(staticName = "create")
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
