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

import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.PipelineStep;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.OccurrenceInterpretationPipeline;

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

    // Run interpretation
    OccurrenceInterpretationPipeline.runInterpretation(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        pipelinesConfig.getStandalone().getNumberOfShards(),
        message.getValidationResult().isTripletValid(),
        message.getValidationResult().isOccurrenceIdValid(),
        new ArrayList<>(message.getInterpretTypes())); // map to enum
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
    return TrackingInfo.builder().executionId(message.getExecutionId()).build();
  }

  /** Not applicable for this callback */
  @Override
  protected void updateTrackingStatus(
      TrackingInfo trackingInfo, PipelinesVerbatimMessage message, PipelineStep.Status status) {}
}
