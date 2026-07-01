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
import org.gbif.pipelines.spark.IdentifiersPipeline;
import org.gbif.pipelines.util.SparkConfUtil;

@Slf4j
public class ValidatorIdentifierCallback
    extends PipelinesCallback<PipelinesVerbatimMessage, PipelinesVerbatimMessage>
    implements MessageCallback<PipelinesVerbatimMessage> {

  public ValidatorIdentifierCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER;
  }

  @Override
  protected void configSparkSession(SparkSession.Builder sparkBuilder, PipelinesConfig config) {
    IdentifiersPipeline.configSparkSession(sparkBuilder, config);
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {

    Long recordsNumber = getRecordsNumber(pipelinesConfig, message, fileSystem);
    int numberOfShards = SparkConfUtil.getNumberOfShards(pipelinesConfig, recordsNumber);

    // run pipeline
    IdentifiersPipeline.runValidation(
        sparkSession,
        fileSystem,
        pipelinesConfig,
        message.getDatasetUuid().toString(),
        message.getAttempt(),
        numberOfShards,
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
      log.info(validationResult.validationMessage());
    } else {
      // Registry notifications don't apply to validator runs (no registered dataset/execution).
      if (!isRegistryBypassed()) {
        historyClient.notifyAbsentIdentifiers(
            message.getDatasetUuid(),
            message.getAttempt(),
            message.getExecutionId(),
            validationResult.validationMessage());
        historyClient.markPipelineStatusAsAborted(message.getExecutionId());
      }
      log.error(validationResult.validationMessage());
      throw new PipelinesException(validationResult.validationMessage());
    }
  }

  @Override
  protected String getMetaFileName() {
    return IdentifiersPipeline.METRICS_FILENAME;
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
