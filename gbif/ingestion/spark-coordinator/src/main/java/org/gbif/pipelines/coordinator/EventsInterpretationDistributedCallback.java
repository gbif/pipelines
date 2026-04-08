package org.gbif.pipelines.coordinator;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class EventsInterpretationDistributedCallback extends EventsInterpretationCallback {

  public EventsInterpretationDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher, null);
  }

  @Override
  protected void runPipeline(PipelinesEventsMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "events-interpretation",
        fileSystem,
        pipelinesConfig.getAirflowConfig().eventsInterpretationDag,
        StepType.EVENTS_VERBATIM_TO_INTERPRETED);
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
