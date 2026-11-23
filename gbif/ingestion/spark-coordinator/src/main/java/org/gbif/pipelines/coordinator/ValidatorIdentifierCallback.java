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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.orchestrator.OrchestratorReplies;
import org.gbif.pipelines.spark.IdentifiersPipeline;
import org.gbif.pipelines.util.SparkConfUtil;

@Slf4j
public class ValidatorIdentifierCallback
    extends ValidatorCallback<PipelinesVerbatimMessage, PipelinesVerbatimMessage>
    implements MessageCallback<PipelinesVerbatimMessage> {

  public ValidatorIdentifierCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher, String master) {
    super(pipelinesConfig, publisher, master);
  }

  @Override
  protected StepType getStepType() {
    return StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER;
  }

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
        message.getValidationResult().getUseExtendedRecordId() != null
            && message.getValidationResult().getUseExtendedRecordId());

    IdentifierValidationResult validationResult =
        PostprocessValidation.builder()
            .httpClient(this.httpClient)
            .message(message)
            .fileSystem(this.fileSystem)
            .config(pipelinesConfig.getStandalone())
            .outputPath(pipelinesConfig.getOutputPath())
            .bypassRegistry(true)
            .build()
            .validate();

    checkValidity(message, validationResult);
  }

  /** Results of orchestrated runs, from the check until the reply is built. */
  private final Map<PipelinesVerbatimMessage, IdentifierValidationResult> orchestratedResults =
      Collections.synchronizedMap(new IdentityHashMap<>());

  /**
   * Runs for validator-ws fail on bad identifiers, as before. For the flow-orchestrator the result
   * is reported as {@code IDENTIFIERS} and the orchestrator decides (it applies the same rule).
   */
  protected void checkValidity(
      PipelinesVerbatimMessage message, IdentifierValidationResult validationResult) {
    if (OrchestratorReplies.header(message).isEmpty()) {
      checkValidity(validationResult);
    } else {
      log.info("Identifier validation for the orchestrator: {}", validationResult);
      orchestratedResults.put(message, validationResult);
    }
  }

  @Override
  protected OrchestratorResult orchestratorResult(PipelinesVerbatimMessage message) {
    IdentifierValidationResult result = orchestratedResults.remove(message);
    if (result == null) {
      throw new IllegalStateException(
          "No identifier validation result for " + message.getDatasetUuid());
    }
    JsonNode content = OrchestratorJson.mapper().valueToTree(result);
    return new OrchestratorResult(
        Map.of(), List.of(new Report(ReportTypes.IDENTIFIERS, content, null)));
  }

  /**
   * Checks the validity of the identifier validation result. If the result is valid but contains
   * duplicate identifiers, a warning is logged and a PipelinesException is thrown. If the result is
   * invalid, an error is logged and a PipelinesException is thrown.
   *
   * @param validationResult
   */
  protected void checkValidity(IdentifierValidationResult validationResult) {
    if (validationResult.isResultValid()) {
      if (validationResult.duplicateIdentifiers() > 0) {
        log.warn(validationResult.validationMessage());
        throw new PipelinesException(
            String.format(
                "Duplicate %d identifiers found. Check field mapping.",
                validationResult.duplicateIdentifiers()));
      } else {
        log.info(validationResult.validationMessage());
      }
    } else {
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
