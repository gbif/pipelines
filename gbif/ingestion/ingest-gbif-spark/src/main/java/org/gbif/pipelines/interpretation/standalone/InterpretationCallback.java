package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.*;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Interpretation;

@Slf4j
public class InterpretationCallback
    extends PipelinesCallback<PipelinesVerbatimMessage, PipelinesInterpretedMessage>
    implements MessageCallback<PipelinesVerbatimMessage> {

  public InterpretationCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    Interpretation.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VERBATIM_TO_INTERPRETED;
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {

    // Run interpretation
    Interpretation.runInterpretation(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        pipelinesConfig.getStandalone().getNumberOfShards(),
        message.getValidationResult().isTripletValid(),
        message.getValidationResult().isOccurrenceIdValid(),
        false);
  }

  @Override
  protected String getMetaFileName() {
    return Interpretation.METRICS_FILENAME;
  }

  public PipelinesInterpretedMessage createOutgoingMessage(PipelinesVerbatimMessage message) {

    Long recordsNumber = null;
    Long eventRecordsNumber = null;
    if (message.getValidationResult() != null) {
      recordsNumber = message.getValidationResult().getNumberOfRecords();
      eventRecordsNumber = message.getValidationResult().getNumberOfEventRecords();
    }

    // boolean repeatAttempt = pathExists(message);
    return new PipelinesInterpretedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        recordsNumber,
        eventRecordsNumber,
        null, // Set in balancer cli
        false, // repeatAttempt,
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getEndpointType(),
        message.getValidationResult(),
        message.getInterpretTypes(),
        message.getDatasetType());
  }

  @Override
  public Class<PipelinesVerbatimMessage> getMessageClass() {
    return PipelinesVerbatimMessage.class;
  }
}
