package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.spark.ArgsConstants.OCCURRENCE_ID_VALID_ARG;
import static org.gbif.pipelines.spark.ArgsConstants.TRIPLET_VALID_ARG;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.util.DistributedUtil;

@Slf4j
public class FragmenterDistributedCallback extends FragmenterCallback {

  public FragmenterDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher, null);
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "fragmenter",
        fileSystem,
        pipelinesConfig.getAirflowConfig().fragmenterDag,
        StepType.FRAGMENTER,
        List.of(
            TRIPLET_VALID_ARG + "=" + message.getValidationResult().isTripletValid(),
            OCCURRENCE_ID_VALID_ARG + "=" + message.getValidationResult().isOccurrenceIdValid()));
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
