package org.gbif.validator.service;

import java.util.UUID;
import lombok.experimental.UtilityClass;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.file.DataFile;

/** Utility class to create instances of API classes. */
@UtilityClass
public class ValidationFactory {

  static Validation newValidationInstance(
      UUID key, DataFile dataFile, String userName, Validation.Status status) {
    return Validation.builder()
        .key(key)
        .fileFormat(dataFile.getFileFormat())
        .status(status)
        .file(dataFile.getFilePath().getFileName().toString())
        .fileSize(dataFile.getSize())
        .username(userName)
        .build();
  }

  static Validation newValidationInstance(UUID key, DataFile dataFile, Validation.Status status) {
    return Validation.builder()
        .key(key)
        .fileFormat(dataFile.getFileFormat())
        .status(status)
        .file(dataFile.getFilePath().getFileName().toString())
        .fileSize(dataFile.getSize())
        .build();
  }

  static Validation newValidationInstance(UUID key, Validation.Status status, Metrics metrics) {
    return Validation.builder().key(key).status(status).metrics(metrics).build();
  }

  static Metrics metricsFromError(String errorMessage) {
    return Metrics.builder()
        .archiveValidationReport(
            Metrics.ArchiveValidationReport.builder().invalidationReason(errorMessage).build())
        .build();
  }
}
