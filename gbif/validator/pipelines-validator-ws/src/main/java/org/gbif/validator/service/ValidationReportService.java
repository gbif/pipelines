package org.gbif.validator.service;

import java.util.UUID;

public interface ValidationReportService {

  void createOrUpdate(UUID key, int attempt, String reportType, String report);

  String get(UUID key, int attempt, String reportType);
}
