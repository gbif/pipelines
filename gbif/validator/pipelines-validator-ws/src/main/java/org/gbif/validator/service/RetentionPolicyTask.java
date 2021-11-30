package org.gbif.validator.service;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesCleanerMessage;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.persistence.mapper.ValidationMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class RetentionPolicyTask {

  private final ValidationMapper validationMapper;

  private final MessagePublisher messagePublisher;

  @Value("${retentionPolicy.keepDays}")
  private final int keepDays;

  @Scheduled(cron = "${retentionPolicy.cron}")
  public void cleanOutdatedData() {
    log.info("Running retention policy cleaner task");
    getDatasetsUuidForDeletion().forEach(this::sendMessage);
  }

  @SneakyThrows
  private void sendMessage(Validation validation) {
    PipelinesCleanerMessage message = new PipelinesCleanerMessage();
    message.setValidator(true);
    message.setDatasetUuid(validation.getKey());
    message.setAttempt(1);

    log.info("Send the message to the validator cleaner queue for key - {}", validation.getKey());
    messagePublisher.send(message);
  }

  private List<Validation> getDatasetsUuidForDeletion() {
    LocalDate toDate = LocalDate.now().minusDays(keepDays);
    return validationMapper.list(
        null, ValidationSearchRequest.builder().toDate(new Date(toDate.toEpochDay())).build());
  }
}
