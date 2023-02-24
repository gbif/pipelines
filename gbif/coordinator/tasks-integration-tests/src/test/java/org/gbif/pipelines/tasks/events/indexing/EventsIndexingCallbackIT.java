package org.gbif.pipelines.tasks.events.indexing;

import static org.gbif.api.model.pipelines.StepType.EVENTS_INTERPRETED_TO_INDEX;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.PipelinesHistoryClientTestStub;
import org.gbif.registry.ws.client.DatasetClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventsIndexingCallbackIT {
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static CloseableHttpClient httpClient;
  @Mock private static DatasetClient datasetClient;

  @Test
  public void invalidMessageTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();

    EventsIndexingConfiguration config = new EventsIndexingConfiguration();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/occurrence/").getFile();
    config.pipelinesConfig = "pipelines.yaml";

    EventsIndexingCallback callback =
        EventsIndexingCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .httpClient(httpClient)
            .historyClient(historyClient)
            .datasetClient(datasetClient)
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    PipelinesEventsInterpretedMessage message =
        new PipelinesEventsInterpretedMessage(
            uuid,
            attempt,
            Collections.singleton(EVENTS_INTERPRETED_TO_INDEX.name()),
            0L,
            0L,
            null,
            null,
            EndpointType.DWC_ARCHIVE,
            Collections.singleton(RecordType.EVENT.name()),
            false,
            StepRunner.DISTRIBUTED.name());

    // When
    callback.handleMessage(message);

    // Should
    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());
  }
}
