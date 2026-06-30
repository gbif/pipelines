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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.util.DistributedUtil;

@Slf4j
public class ValidatorIdentifierDistributedCallback extends IdentifierCallback {

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
        pipelinesConfig.getAirflowConfig().identifierDag,
        StepType.VERBATIM_TO_IDENTIFIER,
        List.of(
            "--tripletValid=" + message.getValidationResult().isTripletValid(),
            "--occurrenceIdValid=" + message.getValidationResult().isOccurrenceIdValid(),
            "--useExtendedRecordId="
                + (message.getValidationResult().isUseExtendedRecordId() != null
                    ? message.getValidationResult().isUseExtendedRecordId()
                    : false)));
  }

  @Override
  protected boolean isStandalone() {
    return false;
  }
}
