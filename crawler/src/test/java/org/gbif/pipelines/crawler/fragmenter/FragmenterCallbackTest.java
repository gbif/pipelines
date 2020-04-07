package org.gbif.pipelines.crawler.fragmenter;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.crawler.HbaseConnectionStub;
import org.gbif.pipelines.crawler.MessagePublisherStub;
import org.gbif.pipelines.keygen.config.KeygenConfig;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryWsClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FragmenterCallbackTest {

  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final String INPUT_DATASET_FOLDER = "/dataset/dwca";
  private static final String FRAGMENTER_LABEL = StepType.FRAGMENTER.getLabel();
  public static final KeygenConfig CFG =
      KeygenConfig.create("test_occurrence", "test_occurrence_counter", "test_occurrence_lookup", null);
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryWsClient client;

  @BeforeClass
  public static void setUp() throws Exception {

    server = new TestingServer();
    curator = CuratorFrameworkFactory.builder()
        .connectString(server.getConnectString())
        .namespace("crawler")
        .retryPolicy(new RetryOneTime(1))
        .build();
    curator.start();

    publisher = MessagePublisherStub.create();

    client = Mockito.mock(PipelinesHistoryWsClient.class);
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

  @Ignore("NOT READY")
  @Test
  public void testNormalCase() throws Exception {
    // State
    FragmenterConfiguration config = new FragmenterConfiguration();
    config.dwcaArchiveRepository = getClass().getResource(INPUT_DATASET_FOLDER).getFile();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/").getFile();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 2;
    String crawlId = DATASET_UUID + "_" + attempt;
    int expSize = 1534;
    EndpointType endpointType = EndpointType.DWC_ARCHIVE;

    PipelinesInterpretedMessage message = new PipelinesInterpretedMessage(
        uuid,
        attempt,
        Collections.singleton(StepType.FRAGMENTER.name()),
        (long) expSize,
        StepRunner.STANDALONE.name(),
        true,
        null,
        null,
        null,
        endpointType
    );

    FragmenterCallback callback = new FragmenterCallback(
        config,
        publisher,
        curator,
        client,
        Executors.newSingleThreadExecutor(),
        HbaseConnectionStub.create(),
        CFG);

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(checkExists(curator, crawlId, FRAGMENTER_LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(FRAGMENTER_LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(FRAGMENTER_LABEL)));
    assertEquals(1, publisher.getMessages().size());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, FRAGMENTER_LABEL));
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }

}
