package org.gbif.pipelines.coordinator;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.DataPackageConversionPipeline;

@Slf4j
public class DwcDpStageCallback extends PipelinesCallback<DwcDpStageMessage, PipelineBasedMessage>
    implements CloseableMessageCallback<DwcDpStageMessage> {

  public DwcDpStageCallback(PipelinesConfig config, MessagePublisher publisher, String master) {
    super(config, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.DWCDP_STAGE;
  }

  @Override
  protected void runPipeline(DwcDpStageMessage message) throws Exception {
    String datasetId = message.getDatasetUuid().toString();
    int attempt = message.getAttempt();

    DataPackageConversionPipeline.runCopy(
        new DataPackageConversionPipeline.CopyConfig(
            sparkSession,
            pipelinesConfig.getDwcdpNfsRepository(),
            pipelinesConfig.getOutputPath(),
            datasetId,
            attempt,
            pipelinesConfig.getPartitionSizeInMB() * 1024L * 1024L));
  }

  @Override
  protected String getMetaFileName() {
    return PipelinesVariables.Pipeline.DWCDP_STAGE + ".yml";
  }

  @Override
  public Class<DwcDpStageMessage> getMessageClass() {
    return DwcDpStageMessage.class;
  }

  @Override
  public PipelineBasedMessage createOutgoingMessage(DwcDpStageMessage message) {
    return new DwcDpToVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getExecutionId(),
        message.isContainsOccurrences(),
        message.isContainsEvents(),
        isStandalone());
  }
}
