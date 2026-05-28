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
package org.gbif.pipelines.tasks.validators.validator.validator;

import java.util.Collections;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesValidatorArchiveValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.modes.CallbackModeType;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.tasks.validators.validator.validate.ArchiveValidatorFactory;
import org.gbif.pipelines.tasks.validators.validator.validate.PipelinesValidatorDwcaArchiveValidatorOutgoingMessageCreator;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesArchiveValidatorMessage} is received. */
@Slf4j
@AllArgsConstructor
public class PipelinesValidatorArchiveValidatorCallback
    extends AbstractMessageCallback<PipelinesValidatorArchiveValidatorMessage>
    implements StepHandler<PipelinesValidatorArchiveValidatorMessage, PipelineBasedMessage> {

  private final ArchiveValidatorConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;

  @Override
  public void handleMessage(PipelinesValidatorArchiveValidatorMessage message) {
    PipelinesCallback.<PipelinesValidatorArchiveValidatorMessage, PipelineBasedMessage>builder()
        .historyClient(historyClient)
        .validationClient(validationClient)
        .config(config)
        .stepType(StepType.VALIDATOR_VALIDATE_ARCHIVE)
        .callbackModeType(CallbackModeType.VALIDATOR)
        .publisher(publisher)
        .message(message)
        .handler(this)
        .build()
        .handleMessage();
  }

  @Override
  public String getRouting() {
    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    if (config.validatorOnly) {
      message.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_VALIDATE_ARCHIVE.name()));
    }
    return message.getRoutingKey();
  }

  @Override
  public boolean isMessageCorrect(PipelinesValidatorArchiveValidatorMessage message) {
    return message.getFileFormat() != null && message.getDatasetUuid() != null;
  }

  @Override
  public Runnable createRunnable(PipelinesValidatorArchiveValidatorMessage message) {
    return () -> {
      log.info("Running validation for {}", message.getDatasetUuid());
      ArchiveValidatorFactory.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .schemaValidatorFactory(schemaValidatorFactory)
          .outgoingMessageCreator(
              new PipelinesValidatorDwcaArchiveValidatorOutgoingMessageCreator())
          .build()
          .create()
          .validate();
    };
  }

  @SneakyThrows
  @Override
  public PipelineBasedMessage createOutgoingMessage(
      PipelinesValidatorArchiveValidatorMessage message) {
    return ArchiveValidatorFactory.builder()
        .message(message)
        .config(config)
        .outgoingMessageCreator(new PipelinesValidatorDwcaArchiveValidatorOutgoingMessageCreator())
        .build()
        .create()
        .createOutgoingMessage();
  }
}
