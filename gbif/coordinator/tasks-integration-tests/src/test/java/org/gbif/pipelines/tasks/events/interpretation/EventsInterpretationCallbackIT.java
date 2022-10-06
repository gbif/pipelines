package org.gbif.pipelines.tasks.events.interpretation;

import static org.gbif.api.model.pipelines.StepType.EVENTS_INTERPRETED_TO_INDEX;
import static org.gbif.api.model.pipelines.StepType.EVENTS_VERBATIM_TO_INTERPRETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.utils.CuratorServer;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventsInterpretationCallbackIT {

  @ClassRule public static final CuratorServer CURATOR_SERVER = new CuratorServer();
  private static final String EVENTS_INTERPRETED_LABEL = EVENTS_VERBATIM_TO_INTERPRETED.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static PipelinesHistoryClient historyClient;

  @Test
  public void testInvalidMessage() {

    // State
    EventsInterpretationConfiguration config = new EventsInterpretationConfiguration();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/occurrence/").getFile();
    config.pipelinesConfig = "pipelines.yaml";

    EventsInterpretationCallback callback =
        new EventsInterpretationCallback(
            config, PUBLISHER, CURATOR_SERVER.getCurator(), historyClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;
    ValidationResult validationResult = new ValidationResult(true, true, false, 0L, null);

    PipelinesEventsMessage message =
        new PipelinesEventsMessage(
            uuid,
            attempt,
            new HashSet<>(
                Arrays.asList(
                    EVENTS_VERBATIM_TO_INTERPRETED.name(), EVENTS_INTERPRETED_TO_INDEX.name())),
            0L,
            0L,
            null,
            false,
            null,
            null,
            EXECUTION_ID,
            EndpointType.DWC_ARCHIVE,
            validationResult,
            Collections.singleton(RecordType.EVENT.name()),
            DatasetType.SAMPLING_EVENT);

    // When
    callback.handleMessage(message);

    // Should
    Path path =
        Paths.get(
            config.stepConfig.repositoryPath
                + DATASET_UUID
                + "/"
                + attempt
                + "/"
                + DwcTerm.Event.name().toLowerCase());
    assertFalse(path.toFile().exists());
    assertFalse(CURATOR_SERVER.checkExists(crawlId, EVENTS_INTERPRETED_LABEL));
    assertFalse(
        CURATOR_SERVER.checkExists(crawlId, Fn.SUCCESSFUL_MESSAGE.apply(EVENTS_INTERPRETED_LABEL)));
    assertFalse(
        CURATOR_SERVER.checkExists(crawlId, Fn.MQ_CLASS_NAME.apply(EVENTS_INTERPRETED_LABEL)));
    assertFalse(CURATOR_SERVER.checkExists(crawlId, Fn.MQ_MESSAGE.apply(EVENTS_INTERPRETED_LABEL)));
    assertEquals(0, PUBLISHER.getMessages().size());
  }
}
