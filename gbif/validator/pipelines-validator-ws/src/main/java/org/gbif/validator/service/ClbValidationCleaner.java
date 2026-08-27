package org.gbif.validator.service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.mail.validator.ValidatorEmailService;
import org.gbif.pipelines.validator.ChecklistValidator;
import org.gbif.pipelines.validator.ws.ChecklistbankWsClient;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.persistence.mapper.ValidationMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * This service looks for checklist validations that are still waiting for CLB API response after a
 * certain period of time and updates its state.
 */
@SuppressWarnings("unchecked")
@Slf4j
@Component
@RequiredArgsConstructor
public class ClbValidationCleaner {

  private final ChecklistbankWsClient checklistbankWsClient;
  private final ValidatorEmailService emailService;
  private final ValidationMapper validationMapper;
  private final MessagePublisher messagePublisher;
  private final ChecklistValidator checklistValidator;

  @Value("${clb.cleaner.hoursOld}")
  private final int hoursOld;

  @Scheduled(cron = "${clb.cleaner.cron}")
  public void cleanClbValidations() {
    // oldest date to check
    Date toDate =
        Date.from(
            LocalDateTime.now().atZone(ZoneId.systemDefault()).minusHours(hoursOld).toInstant());

    log.info("Cleaning validations that are {} hours old until {}", hoursOld, toDate);

    List<Validation> validationsWaitingForClbApi =
        validationMapper.list(
            null,
            ValidationSearchRequest.builder()
                .status(Set.of(Validation.Status.WAITING_FOR_CHECKLISTBANK))
                .toDate(toDate)
                .build());

    validationsWaitingForClbApi.stream()
        .filter(v -> v.getClbDatasetKey() != null)
        .forEach(
            validation -> {
              try {
                List<ChecklistbankWsClient.ImportResponse> importResponses =
                    checklistbankWsClient.checkImport(validation.getClbDatasetKey());
                ChecklistbankWsClient.ImportResponse importResponse =
                    (importResponses != null && !importResponses.isEmpty())
                        ? importResponses.get(0)
                        : null;

                // set to FAILED by default
                setStatus(validation, Validation.Status.FAILED);

                if (importResponse != null && importResponse.getStatus() != null) {
                  if (importResponse.getStatus().equalsIgnoreCase(ClbDatasetImport.FINISHED)) {
                    setStatus(validation, Validation.Status.FINISHED);

                    // send message to continue the process
                    messagePublisher.send(
                        checklistValidator.createNextMessage(validation.getClbValidationMessage()));
                  } else if (importResponse
                      .getStatus()
                      .equalsIgnoreCase(ClbDatasetImport.CANCELED)) {
                    setStatus(validation, Validation.Status.ABORTED);
                  }
                }
              } catch (Exception e) {
                log.warn("Error getting clb validations for key {}", validation.getKey(), e);
                setStatus(validation, Validation.Status.FAILED);
              }

              validationMapper.update(validation);
              emailService.sendEmailNotification(validation);
            });
  }

  private void setStatus(Validation validation, Validation.Status newStatus) {
    validation.getMetrics().getStepTypes().stream()
        .filter(step -> step.getStepType().equals(StepType.VALIDATOR_VALIDATE_ARCHIVE.name()))
        .forEach(step -> step.setStatus(newStatus));
    validation.setStatus(newStatus);
  }
}
