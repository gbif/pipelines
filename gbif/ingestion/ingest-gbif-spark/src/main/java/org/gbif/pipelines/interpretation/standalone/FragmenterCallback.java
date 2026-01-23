package org.gbif.pipelines.interpretation.standalone;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesFragmenterMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.Fragmenter;

@Slf4j
public class FragmenterCallback
    extends PipelinesCallback<PipelinesInterpretedMessage, PipelinesFragmenterMessage>
    implements MessageCallback<PipelinesInterpretedMessage>, AutoCloseable {

  public FragmenterCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    Fragmenter.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected StepType getStepType() {
    return StepType.FRAGMENTER;
  }

  @Override
  protected void runPipeline(PipelinesInterpretedMessage message) throws Exception {
    Fragmenter.runFragmenter(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        message.getValidationResult().isTripletValid(),
        message.getValidationResult().isOccurrenceIdValid());
  }

  @Override
  protected String getMetaFileName() {
    return Fragmenter.METRICS_FILENAME;
  }

  @Override
  public Class<PipelinesInterpretedMessage> getMessageClass() {
    return PipelinesInterpretedMessage.class;
  }

  @Override
  public PipelinesFragmenterMessage createOutgoingMessage(PipelinesInterpretedMessage message) {
    return new PipelinesFragmenterMessage(
        message.getDatasetUuid(), message.getAttempt(), message.getPipelineSteps());
  }
}
