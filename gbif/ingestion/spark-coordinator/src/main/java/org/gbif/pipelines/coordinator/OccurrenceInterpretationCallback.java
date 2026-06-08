package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.util.DistributedUtil.getRecordsNumber;

import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.*;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.OccurrenceInterpretationPipeline;
import org.gbif.pipelines.util.CleanupUtil;
import org.gbif.pipelines.util.SparkConfUtil;

@Slf4j
public class OccurrenceInterpretationCallback
    extends PipelinesCallback<PipelinesVerbatimMessage, PipelinesInterpretedMessage>
    implements MessageCallback<PipelinesVerbatimMessage> {

  public OccurrenceInterpretationCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    OccurrenceInterpretationPipeline.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VERBATIM_TO_INTERPRETED;
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {

    Long recordsNumber = getRecordsNumber(pipelinesConfig, message, fileSystem);
    int numberOfShards = SparkConfUtil.getNumberOfShards(pipelinesConfig, recordsNumber);

    // Run interpretation
    OccurrenceInterpretationPipeline.runInterpretation(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        numberOfShards,
        message.getValidationResult().isTripletValid(),
        message.getValidationResult().isOccurrenceIdValid(),
        new ArrayList<>(message.getInterpretTypes())); // map to enum

    // After a successful run, cleanup previous attempts
    CleanupUtil.cleanupPreviousOnSuccess(
        pipelinesConfig,
        fileSystem,
        message.getDatasetUuid().toString(),
        Set.of(message.getAttempt().toString()));
  }

  @Override
  protected String getMetaFileName() {
    return OccurrenceInterpretationPipeline.METRICS_FILENAME;
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
