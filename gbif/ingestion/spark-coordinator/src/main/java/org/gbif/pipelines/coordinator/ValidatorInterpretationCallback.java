/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.pipelines.coordinator;

import static org.gbif.pipelines.util.DistributedUtil.getRecordsNumber;

import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.Directories;
import org.gbif.pipelines.spark.OccurrenceInterpretationPipeline;
import org.gbif.pipelines.util.SparkConfUtil;

@Slf4j
public class ValidatorInterpretationCallback
    extends PipelinesCallback<PipelinesVerbatimMessage, PipelinesInterpretedMessage>
    implements MessageCallback<PipelinesVerbatimMessage> {

  public ValidatorInterpretationCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    OccurrenceInterpretationPipeline.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VALIDATOR_VERBATIM_TO_INTERPRETED;
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {
    log.debug("Run interpretation for validation: {}", message.getDatasetUuid());

    // The validator workflow has no identifiers step, so the verbatim parquet directory that
    // OccurrenceInterpretationPipeline reads as its input is never produced (in the occurrence
    // workflow IdentifiersPipeline writes it). Convert the verbatim.avro file produced by the
    // validator dwca/xml-to-avro step into that verbatim parquet directory before interpreting.
    convertVerbatimAvroToParquet(message);

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
  }

  /**
   * Converts the {@code verbatim.avro} file written by the validator dwca/xml-to-avro step into the
   * {@code verbatim} parquet directory expected by {@link OccurrenceInterpretationPipeline}. In the
   * occurrence workflow this directory is produced by {@code IdentifiersPipeline}, but the
   * validator workflow doesn't run that step.
   */
  private void convertVerbatimAvroToParquet(PipelinesVerbatimMessage message) {
    String basePath =
        String.format(
            "%s/%s/%d",
            pipelinesConfig.getInputPath(), message.getDatasetUuid(), message.getAttempt());

    String avroPath = basePath + "/verbatim.avro";
    String parquetPath = basePath + "/" + Directories.OCCURRENCE_VERBATIM;

    log.info("Converting verbatim avro {} to parquet {}", avroPath, parquetPath);
    sparkSession
        .read()
        .format("avro")
        .load(avroPath)
        .as(Encoders.bean(ExtendedRecord.class))
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(parquetPath);
  }

  @Override
  protected String getMetaFileName() {
    return OccurrenceInterpretationPipeline.METRICS_FILENAME;
  }

  public PipelinesInterpretedMessage createOutgoingMessage(PipelinesVerbatimMessage message) {
    log.debug("Create outgoing message for validation: {}", message.getDatasetUuid());

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

  /** Not applicable for this callback */
  @Override
  protected boolean isMessageCorrect(PipelinesVerbatimMessage message) {
    return true;
  }

  /** Not applicable for this callback */
  @Override
  protected boolean isProcessingStopped(PipelinesVerbatimMessage message) {
    return false;
  }

  /** Not applicable for this callback */
  @Override
  protected TrackingInfo trackPipelineStep(PipelinesVerbatimMessage message) {
    return TrackingInfo.builder().build();
  }

  /** Not applicable for this callback */
  @Override
  protected void updateTrackingStatus(
      TrackingInfo trackingInfo, PipelinesVerbatimMessage message, PipelineStep.Status status) {}
}
