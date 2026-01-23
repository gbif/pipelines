package org.gbif.pipelines.interpretation.standalone;

import static org.gbif.pipelines.interpretation.spark.Directories.EVENT_JSON;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesEventsInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesEventsMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.interpretation.spark.EventInterpretation;
import org.gbif.pipelines.interpretation.spark.Interpretation;

@Slf4j
public class EventsInterpretationCallback
    extends PipelinesCallback<PipelinesEventsMessage, PipelinesEventsInterpretedMessage>
    implements MessageCallback<PipelinesEventsMessage> {

  public EventsInterpretationCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected boolean isMessageCorrect(PipelinesEventsMessage message) {
    log.info("Checking dataset type: {}", message.getDatasetType());
    log.debug("Full message: {}", message);
    return message.getDatasetType() == DatasetType.SAMPLING_EVENT;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    Interpretation.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected StepType getStepType() {
    return StepType.EVENTS_VERBATIM_TO_INTERPRETED;
  }

  @Override
  protected void runPipeline(PipelinesEventsMessage message) throws Exception {

    // Run interpretation
    EventInterpretation.runEventInterpretation(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        pipelinesConfig.getStandalone().getNumberOfShards());
  }

  @Override
  protected String getMetaFileName() {
    return EventInterpretation.METRICS_FILENAME;
  }

  public PipelinesEventsInterpretedMessage createOutgoingMessage(PipelinesEventsMessage message) {

    Boolean repeatAttempt = null;
    try {
      repeatAttempt = pathExists(message, EVENT_JSON);
    } catch (IOException e) {
      log.warn("Failed to check for existing event-json path", e);
    }
    return new PipelinesEventsInterpretedMessage(
        message.getDatasetUuid(),
        message.getAttempt(),
        message.getPipelineSteps(),
        message.getNumberOfOccurrenceRecords(),
        message.getNumberOfEventRecords(),
        message.getResetPrefix(),
        message.getExecutionId(),
        message.getEndpointType(),
        message.getInterpretTypes(),
        repeatAttempt,
        message.getRunner());
  }

  @Override
  public Class<PipelinesEventsMessage> getMessageClass() {
    return PipelinesEventsMessage.class;
  }
}
