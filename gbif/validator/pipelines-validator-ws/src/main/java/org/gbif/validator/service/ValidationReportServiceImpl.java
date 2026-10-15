package org.gbif.validator.service;

import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.validator.persistence.mapper.ValidationReportMapper;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@Slf4j
@Service
public class ValidationReportServiceImpl implements ValidationReportService {

  private final ValidationReportMapper validationReportMapper;

  @Override
  public void createOrUpdate(UUID key, int attempt, String reportType, String report) {
    validationReportMapper.createOrUpdate(key, attempt, reportType, report);

    log.info("Stored validation report for key {}, attempt {}, type {}", key, attempt, reportType);
  }

  @Override
  public String get(UUID key, int attempt, String reportType) {
    String report = validationReportMapper.get(key, attempt, reportType);

    if (report == null) {
      log.debug(
          "Validation report not found for key {}, attempt {}, type {}", key, attempt, reportType);
    }

    return report;
  }
}
