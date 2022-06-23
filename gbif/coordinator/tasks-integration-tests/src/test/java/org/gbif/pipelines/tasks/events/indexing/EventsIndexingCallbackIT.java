package org.gbif.pipelines.tasks.events.indexing;

import static org.gbif.api.model.pipelines.StepType.EVENTS_INTERPRETED_TO_INDEX;
import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class EventsIndexingCallbackIT {

  private static final String EVENTS_INDEXED_LABEL = EVENTS_INTERPRETED_TO_INDEX.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryClient historyClient;
  private static CloseableHttpClient httpClient;

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
    httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();
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
  public void testInvalidMessage() {

    // State
    EventsIndexingConfiguration config = new EventsIndexingConfiguration();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/occurrence/").getFile();
    config.pipelinesConfig = "pipelines.yaml";

    EventsIndexingCallback callback =
        new EventsIndexingCallback(config, publisher, curator, httpClient, historyClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;

    PipelinesEventsInterpretedMessage message =
        new PipelinesEventsInterpretedMessage(
            uuid,
            attempt,
            Collections.singleton(EVENTS_INTERPRETED_TO_INDEX.name()),
            0L,
            0L,
            null,
            StepRunner.DISTRIBUTED.name(),
            EXECUTION_ID,
            EndpointType.DWC_ARCHIVE,
            Collections.singleton(RecordType.EVENT.name()),
            false,
            null);

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, crawlId, EVENTS_INDEXED_LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL_MESSAGE.apply(EVENTS_INDEXED_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(EVENTS_INDEXED_LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(EVENTS_INDEXED_LABEL)));
    assertEquals(0, publisher.getMessages().size());
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path));
  }
}
