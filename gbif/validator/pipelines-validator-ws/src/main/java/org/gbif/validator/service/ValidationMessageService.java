package org.gbif.validator.service;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.validator.api.FileFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ValidationMessageService {

  private final MessagePublisher messagePublisher;
  private final String dwcdpExchange;
  private final String dwcdpRoutingKey;

  public ValidationMessageService(
      MessagePublisher messagePublisher,
      @Value("${validator.dwcdp.exchange}") String dwcdpExchange,
      @Value("${validator.dwcdp.routing-key}") String dwcdpRoutingKey) {
    this.messagePublisher = messagePublisher;
    this.dwcdpExchange = dwcdpExchange;
    this.dwcdpRoutingKey = dwcdpRoutingKey;
  }

  public void send(UUID validationKey, FileFormat fileFormat, Set<StepType> steps)
      throws IOException {

    switch (fileFormat) {
      case DWCDP -> sendDwcdpMessage(validationKey, steps);
      default -> sendArchiveMessage(validationKey, fileFormat, steps);
    }
  }

  private void sendArchiveMessage(UUID validationKey, FileFormat fileFormat, Set<StepType> steps)
      throws IOException {

    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();

    message.setDatasetUuid(validationKey);
    message.setAttempt(1);
    message.setExecutionId(1L);
    message.setPipelineSteps(steps.stream().map(Enum::name).collect(Collectors.toSet()));
    message.setFileFormat(fileFormat.name());

    messagePublisher.send(message);
  }

  private void sendDwcdpMessage(UUID validationKey, Set<StepType> steps) throws IOException {

    Map<String, Object> message = Map.of("datasetUuid", validationKey.toString(), "attempt", 1);

    messagePublisher.send(message, dwcdpExchange, dwcdpRoutingKey);
  }
}
