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
package org.gbif.pipelines.tasks.balancer.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesBalancerMessage;
import org.gbif.common.messaging.api.messages.PipelinesValidatorDwcaMessage;

/**
 * Populates and sends the {@link PipelinesValidatorDwcaMessage} message, the main method is {@link
 * PipelinesValidatorDwcaMessageHandler#handle}
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PipelinesValidatorDwcaMessageHandler {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Main handler, basically computes the runner type and sends to the same consumer */
  public static void handle(MessagePublisher publisher, PipelinesBalancerMessage message)
      throws IOException {

    log.info("Process PipelinesValidatorDwcaMessage - {}", message);

    PipelinesValidatorDwcaMessage m =
        MAPPER.readValue(message.getPayload(), PipelinesValidatorDwcaMessage.class);

    PipelinesValidatorDwcaMessage outputMessage =
        new PipelinesValidatorDwcaMessage(
            m.getDatasetUuid(),
            m.getDatasetType(),
            m.getSource(),
            m.getAttempt(),
            m.getValidationReport(),
            m.getPipelineSteps(),
            m.getEndpointType(),
            m.getPlatform(),
            m.getExecutionId());

    publisher.send(outputMessage);

    if (log.isTraceEnabled()) {
      log.trace("The message has been sent - {}", outputMessage);
    }

    log.info(
        "Outgoing dataset: {}, executionID: {}, routingKey: {}, attempt: {}",
        outputMessage.getDatasetUuid(),
        outputMessage.getExecutionId(),
        outputMessage.getRoutingKey(),
        outputMessage.getAttempt());
  }
}
