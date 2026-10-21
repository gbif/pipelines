package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.spark.ArgsConstants.*;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.util.DistributedUtil;

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
        StepType.VERBATIM_TO_IDENTIFIER,
        List.of(
            TRIPLET_VALID_ARG + "=" + message.getValidationResult().isTripletValid(),
            OCCURRENCE_ID_VALID_ARG + "=" + message.getValidationResult().isOccurrenceIdValid(),
            USE_EXTENDED_RECORD_ID_ARG
                + "="
                + (message.getValidationResult().getUseExtendedRecordId() != null
                    ? message.getValidationResult().getUseExtendedRecordId()
                    : false)));
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
