package org.gbif.pipelines.tasks.events.indexing;

import static org.gbif.api.model.pipelines.StepType.EVENTS_INTERPRETED_TO_INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.UUID;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.resources.CuratorServer;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventsIndexingCallbackIT {

  @ClassRule public static final CuratorServer CURATOR_SERVER = CuratorServer.getInstance();
  private static final String EVENTS_INDEXED_LABEL = EVENTS_INTERPRETED_TO_INDEX.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static PipelinesHistoryClient historyClient;
  @Mock private static CloseableHttpClient httpClient;
  @Mock private static CloseableHttpClient httpClient;

  @Test
  public void testInvalidMessage() {

    // State
    EventsIndexingConfiguration config = new EventsIndexingConfiguration();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/occurrence/").getFile();
    config.pipelinesConfig = "pipelines.yaml";

    EventsIndexingCallback callback =
        EventsIndexingCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .curator(CURATOR_SERVER.getCurator())
            .httpClient(httpClient)
            .historyClient(historyClient)
            .datasetClient(datasetClient)
            .build();

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
    assertFalse(CURATOR_SERVER.checkExists(crawlId, EVENTS_INDEXED_LABEL));
    assertFalse(
        CURATOR_SERVER.checkExists(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(EVENTS_INDEXED_LABEL)));
    assertFalse(CURATOR_SERVER.checkExists(crawlId, Fn.MQ_CLASS_NAME.apply(EVENTS_INDEXED_LABEL)));
    assertFalse(CURATOR_SERVER.checkExists(crawlId, Fn.MQ_MESSAGE.apply(EVENTS_INDEXED_LABEL)));
    assertEquals(0, PUBLISHER.getMessages().size());
  }
}
