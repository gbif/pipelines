package org.gbif.pipelines.coordinator;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;

@Slf4j
public class IdentifierDistributedCallback extends IdentifierCallback {

  public IdentifierDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher, null);
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "identifiers",
        fileSystem,
        pipelinesConfig.getAirflowConfig().identifierDag,
        StepType.VERBATIM_TO_IDENTIFIER);
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
