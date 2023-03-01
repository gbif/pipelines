package org.gbif.validator.service;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import lombok.experimental.UtilityClass;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.ws.file.DataFile;

/** Utility class to create instances of API classes. */
@UtilityClass
public class ValidationFactory {

  static Validation newValidationInstance(
      UUID key,
      DataFile dataFile,
      String userName,
      Validation.Status status,
      String sourceId,
      UUID installationKey,
      Set<String> notificationEmails) {
    return Validation.builder()
        .key(key)
        .fileFormat(dataFile.getFileFormat())
        .status(status)
        .file(dataFile.getFilePath().getFileName().toString())
        .fileSize(dataFile.getSize())
        .username(userName)
        .installationKey(installationKey)
        .sourceId(sourceId)
        .notificationEmails(notificationEmails)
        .metrics(
            Metrics.builder()
                .stepTypes(
                    Collections.singletonList(
                        ValidationStep.builder()
                            .stepType(StepType.VALIDATOR_UPLOAD_ARCHIVE.name())
                            .status(Status.RUNNING)
                            .executionOrder(
                                PipelinesWorkflow.getValidatorWorkflow()
                                    .getLevel(StepType.VALIDATOR_UPLOAD_ARCHIVE))
                            .build()))
                .build())
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

  static Metrics metricsSubmitError(String errorMessage) {
    return Metrics.builder()
        .stepTypes(StepsMapper.getUploadingSteps(Status.FAILED, errorMessage))
        .build();
  }
}
