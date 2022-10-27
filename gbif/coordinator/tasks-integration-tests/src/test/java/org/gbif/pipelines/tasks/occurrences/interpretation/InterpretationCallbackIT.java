package org.gbif.pipelines.tasks.occurrences.interpretation;

import static org.gbif.api.model.pipelines.StepRunner.DISTRIBUTED;
import static org.gbif.api.model.pipelines.StepRunner.STANDALONE;
import static org.gbif.api.model.pipelines.StepType.VERBATIM_TO_INTERPRETED;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.tasks.CloseableHttpClientStub;
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
public class InterpretationCallbackIT {

  private static final String INTERPRETED_LABEL = VERBATIM_TO_INTERPRETED.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  @Mock private static DatasetClient datasetClient;
  @Mock private static PipelinesHistoryClient historyClient;
  @Mock private static ValidationWsClient validationClient;
  @Mock private static CloseableHttpClient httpClient;

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

    publisher = MessagePublisherStub.create();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    curator.close();
    server.stop();
    publisher.close();
  }

  @After
  public void after() {
    publisher.close();
  }

  @Test
  public void testInvalidMessageRunner() {

    // State
    InterpreterConfiguration config = new InterpreterConfiguration();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/occurrence/").getFile();
    config.processRunner = STANDALONE.name();
    config.pipelinesConfig = "pipelines.yaml";

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    InterpretationCallback callback =
        InterpretationCallback.builder()
            .config(config)
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .httpClient(httpClient)
            .executor(executorService)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;
    ValidationResult validationResult = new ValidationResult(true, true, false, 0L, null);

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
            EXECUTION_ID,
            null);

    // When
    callback.handleMessage(message);

    // Should
    Path path =
        Paths.get(config.stepConfig.repositoryPath + DATASET_UUID + "/" + attempt + "/interpreted");
    assertFalse(path.toFile().exists());
    assertFalse(checkExists(curator, crawlId, INTERPRETED_LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(INTERPRETED_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(INTERPRETED_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(INTERPRETED_LABEL)));
    assertEquals(0, publisher.getMessages().size());
  }

  @Test
  public void testInvalidChildSystemProcess() throws Exception {

    // State
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
            .publisher(publisher)
            .curator(curator)
            .historyClient(historyClient)
            .validationClient(validationClient)
            .httpClient(closeableHttpClient)
            .executor(executorService)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;
    ValidationResult validationResult = new ValidationResult(true, true, false, 0L, null);

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
            EXECUTION_ID,
            null);

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, INTERPRETED_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(INTERPRETED_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(INTERPRETED_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(INTERPRETED_LABEL)));
    assertEquals(0, publisher.getMessages().size());

    // Clean
    curator
        .delete()
        .deletingChildrenIfNeeded()
        .forPath(getPipelinesInfoPath(crawlId, INTERPRETED_LABEL));
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }
}
