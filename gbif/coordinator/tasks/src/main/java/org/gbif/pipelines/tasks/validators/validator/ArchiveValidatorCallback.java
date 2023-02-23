package org.gbif.pipelines.tasks.validators.validator;

import java.util.Collections;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.pipelines.tasks.PipelinesCallback;
import org.gbif.pipelines.tasks.StepHandler;
import org.gbif.pipelines.tasks.validators.validator.validate.ArchiveValidatorFactory;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Callback which is called when the {@link PipelinesArchiveValidatorMessage} is received. */
@Slf4j
@AllArgsConstructor
public class ArchiveValidatorCallback
    extends AbstractMessageCallback<PipelinesArchiveValidatorMessage>
    implements StepHandler<PipelinesArchiveValidatorMessage, PipelineBasedMessage> {

  private final ArchiveValidatorConfiguration config;
  private final MessagePublisher publisher;
  private final PipelinesHistoryClient historyClient;
  private final ValidationWsClient validationClient;
  private final SchemaValidatorFactory schemaValidatorFactory;

  @Override
  public void handleMessage(PipelinesArchiveValidatorMessage message) {
    PipelinesCallback.<PipelinesArchiveValidatorMessage, PipelineBasedMessage>builder()
        .historyClient(historyClient)
        .validationClient(validationClient)
        .config(config)
        .stepType(StepType.VALIDATOR_VALIDATE_ARCHIVE)
        .isValidator(config.validatorOnly)
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
  public boolean isMessageCorrect(PipelinesArchiveValidatorMessage message) {
    return message.getFileFormat() != null && message.getDatasetUuid() != null;
  }

  @Override
  public Runnable createRunnable(PipelinesArchiveValidatorMessage message) {
    return () -> {
      log.info("Running validation for {}", message.getDatasetUuid());
      ArchiveValidatorFactory.builder()
          .validationClient(validationClient)
          .config(config)
          .message(message)
          .schemaValidatorFactory(schemaValidatorFactory)
          .build()
          .create()
          .validate();
    };
  }

  @SneakyThrows
  @Override
  public PipelineBasedMessage createOutgoingMessage(PipelinesArchiveValidatorMessage message) {
    return ArchiveValidatorFactory.builder()
        .message(message)
        .config(config)
        .build()
        .create()
        .createOutgoingMessage();
  }
}
