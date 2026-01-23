package org.gbif.pipelines.interpretation.standalone;

import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.ValidateIdentifiers;

@Slf4j
public class IdentifierCallback
    extends PipelinesCallback<PipelinesVerbatimMessage, PipelinesVerbatimMessage>
    implements MessageCallback<PipelinesVerbatimMessage> {

  public IdentifierCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VERBATIM_TO_IDENTIFIER;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    ValidateIdentifiers.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {

    // run pipeline
    ValidateIdentifiers.runValidation(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        pipelinesConfig.getStandalone().getNumberOfShards(),
        message.getValidationResult().isTripletValid(),
        message.getValidationResult().isOccurrenceIdValid(),
        message.getValidationResult().isUseExtendedRecordId() != null
            ? message.getValidationResult().isUseExtendedRecordId()
            : false);

    IdentifierValidationResult validationResult =
        PostprocessValidation.builder()
            .httpClient(this.httpClient)
            .message(message)
            .fileSystem(this.fileSystem)
            .config(pipelinesConfig.getStandalone())
            .outputPath(pipelinesConfig.getOutputPath())
            .build()
            .validate();

    if (validationResult.isResultValid()) {
      log.debug(validationResult.validationMessage());
    } else {
      historyClient.notifyAbsentIdentifiers(
          message.getDatasetUuid(),
          message.getAttempt(),
          message.getExecutionId(),
          validationResult.validationMessage());
      log.error(validationResult.validationMessage());
      historyClient.markPipelineStatusAsAborted(message.getExecutionId());
      throw new PipelinesException(validationResult.validationMessage());
    }
  }

  @Override
  protected String getMetaFileName() {
    return ValidateIdentifiers.METRICS_FILENAME;
  }

  @Override
  public PipelinesVerbatimMessage createOutgoingMessage(PipelinesVerbatimMessage message) {
    Set<String> pipelineSteps = new HashSet<>(message.getPipelineSteps());
    pipelineSteps.remove(getStepType().name());

    return new PipelinesVerbatimMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getInterpretTypes(),
        pipelineSteps,
        message.getRunner(),
        message.getEndpointType(),
        message.getExtraPath(),
        message.getValidationResult(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getDatasetType());
  }

  @Override
  public Class<PipelinesVerbatimMessage> getMessageClass() {
    return PipelinesVerbatimMessage.class;
  }
}
