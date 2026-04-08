package org.gbif.pipelines.coordinator;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class OccurrenceInterpretationDistributedCallback extends OccurrenceInterpretationCallback {

  public OccurrenceInterpretationDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher, null);
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {

    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "interpretation",
        fileSystem,
        pipelinesConfig.getAirflowConfig().interpretationDag,
        StepType.VERBATIM_TO_INTERPRETED,
        List.of("--interpretTypes=" + String.join(",", message.getInterpretTypes())));
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
