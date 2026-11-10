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

import static org.gbif.pipelines.spark.ArgsConstants.*;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.util.DistributedUtil;

@Slf4j
public class ValidatorIdentifierDistributedCallback extends ValidatorIdentifierCallback {

  public ValidatorIdentifierDistributedCallback(
      PipelinesConfig pipelinesConfig, MessagePublisher publisher) {
    super(pipelinesConfig, publisher, null);
  }

  @Override
  protected void runPipeline(PipelinesVerbatimMessage message) throws Exception {
    DistributedUtil.runPipeline(
        pipelinesConfig,
        message,
        "validator-identifiers",
        fileSystem,
        pipelinesConfig.getAirflowConfig().validatorIdentifiersDag,
        StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
        List.of(
            TRIPLET_VALID_ARG + "=" + message.getValidationResult().isTripletValid(),
            OCCURRENCE_ID_VALID_ARG + "=" + message.getValidationResult().isOccurrenceIdValid(),
            USE_EXTENDED_RECORD_ID_ARG
                + "="
                + (message.getValidationResult().getUseExtendedRecordId() != null
                    && message.getValidationResult().getUseExtendedRecordId())));

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

    checkValidity(validationResult);
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
