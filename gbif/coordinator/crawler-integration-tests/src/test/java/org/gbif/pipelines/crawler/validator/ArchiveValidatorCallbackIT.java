package org.gbif.pipelines.crawler.validator;

import static org.gbif.api.model.pipelines.StepType.VALIDATOR_VALIDATE_ARCHIVE;
import static org.gbif.api.model.pipelines.StepType.VALIDATOR_VERBATIM_TO_INTERPRETED;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.crawler.MessagePublisherStub;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class ArchiveValidatorCallbackIT {

  private static final String LABEL = VALIDATOR_VALIDATE_ARCHIVE.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String DUMMY_URL = "http://some.new.url";
  private static final String INPUT_DATASET_FOLDER = "/dataset/dwca";
  private static final long EXECUTION_ID = 1L;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryClient historyClient;
  private static ValidationWsClient validationClient;

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

    historyClient = Mockito.mock(PipelinesHistoryClient.class);
    validationClient = Mockito.mock(ValidationWsClient.class);
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
  public void testNormalCase() throws Exception {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID;

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            new HashSet<>(
                Arrays.asList(
                    VALIDATOR_VALIDATE_ARCHIVE.name(), VALIDATOR_VERBATIM_TO_INTERPRETED.name())),
            EXECUTION_ID,
            false,
            EndpointType.DWC_ARCHIVE);

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  @Test
  public void testNormalSingleStepCase() {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID;

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            Collections.singleton(VALIDATOR_VALIDATE_ARCHIVE.name()),
            EXECUTION_ID,
            false,
            EndpointType.DWC_ARCHIVE);

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, crawlId, LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertEquals(1, publisher.getMessages().size());
  }

  @Test
  public void testFailedCase() throws Exception {
    // State
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    ArchiveValidatorCallback callback =
        new ArchiveValidatorCallback(
            config,
            publisher,
            curator,
            historyClient,
            validationClient,
            new SchemaValidatorFactory());

    UUID uuid = UUID.randomUUID(); // Use wrong datasetKey
    int attempt = 2;
    String crawlId = uuid.toString();

    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            uuid,
            attempt,
            Collections.singleton(VALIDATOR_VALIDATE_ARCHIVE.name()),
            EXECUTION_ID,
            false,
            EndpointType.DWC_ARCHIVE);

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(publisher.getMessages().isEmpty());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL));
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }
}
