package org.gbif.validator.service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.pipelines.validator.ws.ChecklistbankWsClient;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationSearchRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * This service looks for checklist validations that are still waiting for CLB API response after a
 * certain period of time and updates its state.
 */
@SuppressWarnings("unchecked")
@Component
@RequiredArgsConstructor
public class ClbValidationCleaner {

  private final ChecklistbankWsClient checklistbankWsClient;
  private final ValidationService validationService;

  @Value("${clb.cleaner.hoursOld}")
  private final int hoursOld;

  @Scheduled(cron = "${clb.cleaner.cron}")
  public void cleanClbValidations() {
    // oldest date to check
    Date toDate =
        Date.from(
            LocalDateTime.now().atZone(ZoneId.systemDefault()).minusHours(hoursOld).toInstant());

    PagingResponse<Validation> validationsWaitingForClbApi =
        validationService.list(
            ValidationSearchRequest.builder()
                .status(Set.of(Validation.Status.WAITING_FOR_CHECKLISTBANK))
                .toDate(toDate)
                .build());

    validationsWaitingForClbApi.getResults().stream()
        .filter(v -> v.getClbDatasetKey() != null)
        .forEach(
            validation -> {
              ClbDatasetImport clbDatasetImport =
                  checklistbankWsClient.checkImporter(validation.getClbDatasetKey());

              if (clbDatasetImport == null || clbDatasetImport.getState() == null) {
                validation.setStatus(Validation.Status.FAILED);
              } else if (clbDatasetImport.getState() == ClbDatasetImport.State.finished) {
                validation.setStatus(Validation.Status.FINISHED);
              } else if (clbDatasetImport.getState() == ClbDatasetImport.State.canceled) {
                validation.setStatus(Validation.Status.ABORTED);
              } else {
                validation.setStatus(Validation.Status.FAILED);
              }

              validationService.update(validation);
            });
  }
}
