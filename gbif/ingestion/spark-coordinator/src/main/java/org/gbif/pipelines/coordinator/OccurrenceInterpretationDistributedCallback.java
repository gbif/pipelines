package org.gbif.pipelines.coordinator;

import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.util.CleanupUtil;
import org.gbif.pipelines.util.DistributedUtil;

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

    // After a successful run, cleanup previous attempts
    CleanupUtil.cleanupPreviousOnSuccess(
        pipelinesConfig,
        fileSystem,
        message.getDatasetUuid().toString(),
        Set.of(message.getAttempt().toString()));
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
