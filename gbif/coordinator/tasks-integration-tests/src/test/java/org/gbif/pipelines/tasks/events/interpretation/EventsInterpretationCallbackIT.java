package org.gbif.pipelines.tasks.events.interpretation;

import static org.gbif.api.model.pipelines.StepType.EVENTS_INTERPRETED_TO_INDEX;
import static org.gbif.api.model.pipelines.StepType.EVENTS_VERBATIM_TO_INTERPRETED;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage.ValidationResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.PipelinesHistoryClientTestStub;
import org.gbif.registry.ws.client.DatasetClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventsInterpretationCallbackIT {
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();
  @Mock private static DatasetClient datasetClient;

  @Test
  public void invalidMessageTest() {

    // State
    PipelinesHistoryClientTestStub historyClient = PipelinesHistoryClientTestStub.create();

    EventsInterpretationConfiguration config = new EventsInterpretationConfiguration();
    config.stepConfig.repositoryPath = getClass().getResource("/dataset/occurrence/").getFile();
    config.pipelinesConfig = "pipelines.yaml";

    EventsInterpretationCallback callback =
        EventsInterpretationCallback.builder()
            .config(config)
            .publisher(PUBLISHER)
            .historyClient(historyClient)
            .datasetClient(datasetClient)
            .hdfsConfigs(
                HdfsConfigs.create(
                    config.stepConfig.hdfsSiteConfig, config.stepConfig.coreSiteConfig))
            .build();

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
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
    Assert.assertFalse(path.toFile().exists());

    Map<StepType, PipelineStep> result = historyClient.getStepMap();
    Assert.assertEquals(0, result.size());

    Assert.assertEquals(0, historyClient.getPipelineExecutionMap().size());
    Assert.assertEquals(0, historyClient.getPipelineProcessMap().size());
  }
}
