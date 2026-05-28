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
package org.gbif.pipelines.tasks.validators.validator.validate;

import java.net.URI;
import lombok.SneakyThrows;
import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;

public class PipelinesArchiveValidatorOutgoingMessageCreator
    implements DwcaArchiveValidatorOutgoingMessageCreator {

  @Override
  @SneakyThrows
  public PipelinesDwcaMessage createOutgoingMessage(
      ArchiveValidatorConfiguration config, PipelineBasedMessage message) {
    PipelinesArchiveValidatorMessage m = (PipelinesArchiveValidatorMessage) message;
    PipelinesDwcaMessage out = new PipelinesDwcaMessage();
    out.setDatasetUuid(m.getDatasetUuid());
    out.setAttempt(m.getAttempt());
    out.setSource(new URI(config.stepConfig.registry.wsUrl));
    out.setValidationReport(
        new DwcaValidationReport(
            m.getDatasetUuid(), new OccurrenceValidationReport(1, 1, 0, 1, 0, true)));
    out.setPipelineSteps(m.getPipelineSteps());
    out.setExecutionId(m.getExecutionId());
    DwcaArchiveValidatorOutgoingMessageCreator.getDatasetType(config, m)
        .ifPresent(out::setDatasetType);
    out.setEndpointType(EndpointType.DWC_ARCHIVE);
    return out;
  }
}
