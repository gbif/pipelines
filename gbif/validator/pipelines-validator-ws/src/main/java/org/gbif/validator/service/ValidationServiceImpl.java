package org.gbif.validator.service;

import static org.gbif.validator.service.ValidationFactory.metricsSubmitError;
import static org.gbif.validator.service.ValidationFactory.newValidationInstance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.model.registry.Dataset;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.dwca.validation.MetadataPath;
import org.gbif.mail.validator.ValidatorEmailService;
import org.gbif.metadata.eml.parse.DatasetEmlParser;
import org.gbif.pipelines.validator.ChecklistValidator;
import org.gbif.pipelines.validator.Validations;
import org.gbif.utils.file.CompressionUtil.UnsupportedCompressionType;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.persistence.mapper.ValidationMapper;
import org.gbif.validator.ws.file.DataFile;
import org.gbif.validator.ws.file.FileSizeException;
import org.gbif.validator.ws.file.FileStoreManager;
import org.gbif.ws.security.GbifUserPrincipal;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
@RequiredArgsConstructor
public class ValidationServiceImpl implements ValidationService<MultipartFile> {

  private final FileStoreManager fileStoreManager;

  private final ValidationMapper validationMapper;

  private final MessagePublisher messagePublisher;

  private final ValidatorEmailService emailService;

  private final ChecklistValidator checklistValidator;

  @Value("${maxRunningValidationPerUser}")
  private final int maxRunningValidationPerUser;

  private final ErrorMapper errorMapper;

  /** Asserts the user has not reached the maximum number of executing validations. */
  @Override
  public boolean reachedMaxRunningValidations(String userName) {
    boolean reachedMaximum =
        validationMapper.count(
                userName,
                ValidationSearchRequest.builder().status(Validation.executingStatuses()).build())
            >= maxRunningValidationPerUser;
    if (reachedMaximum) {
      log.info("User {} reached maximum running validations", userName);
    }
    return reachedMaximum;
  }

  public void validateRequest(ValidationRequest validationRequest) {
    if (validationRequest.getInstallationKey() != null
        && (validationRequest.getNotificationEmail() == null
            || validationRequest.getNotificationEmail().isEmpty())) {
      throw errorMapper.apply(Validation.ErrorCode.NOTIFICATION_EMAILS_MISSING);
    }
    if (reachedMaxRunningValidations(getPrincipal().getUsername())) {
      throw errorMapper.apply(Validation.ErrorCode.MAX_RUNNING_VALIDATIONS);
    }
  }

  @Override
  public Validation validateFile(MultipartFile file, ValidationRequest validationRequest) {
    validateRequest(validationRequest);

    log.info("Staring validation for the file {}", file.getOriginalFilename());
    UUID key = UUID.randomUUID();
    FileStoreManager.AsyncDataFileTask task = fileStoreManager.uploadDataFile(file, key.toString());
    task.getTask()
        .whenCompleteAsync(
            (df, tr) -> {
              if (tr == null) {
                log.info("File has been uploaded and decompressed, key {}", key);
                updateAndNotifySubmitted(key, df);
              } else {
                log.error(tr.getMessage(), tr);
                String message = "Error during the file submitting. ";
                if (tr.getCause() instanceof UnsupportedCompressionType) {
                  message += "Please check the archive compression type. ";
                }
                updateFailedValidation(key, message + tr.getCause().getMessage());
              }
            });
    return create(key, task.getStart(), Validation.Status.SUBMITTED, validationRequest);
  }

  private GbifUserPrincipal getPrincipal() {
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    if (authentication != null
        && authentication.isAuthenticated()
        && authentication.getPrincipal() instanceof GbifUserPrincipal) {
      return (GbifUserPrincipal) authentication.getPrincipal();
    }
    throw new SecurityException("User credentials not found");
  }

  @Override
  public Validation validateFileFromUrl(String fileURL, ValidationRequest validationRequest) {
    try {
      validateRequest(validationRequest);

      log.info("Starting validation for the URL {}", fileURL);
      UUID key = UUID.randomUUID();
      String encodedFileURL = EncodingUtil.encode(fileURL);
      Optional<String> redirectedUrl = EncodingUtil.getRedirectedUrl(encodedFileURL);
      if (redirectedUrl.isPresent()) {
        encodedFileURL = redirectedUrl.get();
      }

      // this should also become asynchronous at some point
      FileStoreManager.AsyncDownloadResult downloadResult =
          fileStoreManager.downloadDataFile(
              encodedFileURL,
              key.toString(),
              resultDataFile -> {
                if (resultDataFile == null) {
                  log.error("Downloaded file is null for URL {}, key {}", fileURL, key);
                  updateFailedValidation(key, "Downloaded file from " + fileURL + " is null");
                  return;
                }
                try {
                  log.info(
                      "File has been downloaded and decompressed from URL {}, key {} to path {} with size {} bytes",
                      fileURL,
                      key,
                      resultDataFile.getFilePath().toAbsolutePath(),
                      Files.size(resultDataFile.getFilePath()));
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                updateAndNotifySubmitted(key, resultDataFile);
              },
              err -> {
                log.error("Error processing file", err);
                updateFailedValidation(key, err.getMessage());
              });
      return create(
          key, downloadResult.getDataFile(), Validation.Status.DOWNLOADING, validationRequest);
    } catch (FileSizeException ex) {
      log.error("File limit error", ex);
      throw errorMapper.apply(Validation.ErrorCode.MAX_FILE_SIZE_VIOLATION);
    } catch (IOException ex) {
      log.error("Can't download submitted file", ex);
      throw errorMapper.apply(Validation.ErrorCode.IO_ERROR);
    }
  }

  /**
   * Gets a validation by its key, if exists. Access to a validation to all users
   * gbif/portal-feedback#3844
   */
  @Override
  public Validation get(UUID key) {
    Validation validation = validationMapper.get(key);
    if (validation == null) {
      throw errorMapper.apply(Validation.ErrorCode.NOT_FOUND);
    }
    return validation;
  }

  /** Updates validation data. */
  @Override
  public Validation update(Validation validation) {
    if (validation.getKey() != null) {
      Optional.ofNullable(get(validation.getKey()))
          .ifPresent(
              v ->
                  log.info(
                      "Updating validation key {}, status {}",
                      validation.getKey(),
                      validation.getStatus()));
    } else {
      throw errorMapper.apply(Validation.ErrorCode.NOT_FOUND);
    }
    return updateAndGet(validation);
  }

  /** Cancels a running validation. */
  @Override
  public Validation cancel(UUID key) {
    Validation validation = get(key);
    if (!validation.isExecuting()) {
      throw errorMapper.apply(Validation.ErrorCode.VALIDATION_IS_NOT_EXECUTING);
    }
    log.info("Cancel validation record for key {}", key);
    validation.setStatus(Status.ABORTED);
    fileStoreManager.deleteIfExist(key.toString());
    return updateAndGet(validation);
  }

  @Override
  public void delete(UUID key) {
    UUID keyToDelete = get(key).getKey();
    log.info("Delete validation record for key {}", key);
    // A get is executed to check if the validation exists and current user has access
    validationMapper.delete(keyToDelete);
    fileStoreManager.deleteIfExist(key.toString());
  }

  /** Paged result of validations of a user. */
  @Override
  public PagingResponse<Validation> list(ValidationSearchRequest validationSearchRequest) {
    GbifUserPrincipal principal = getPrincipal();
    long total = validationMapper.count(principal.getUsername(), validationSearchRequest);
    return new PagingResponse<>(
        validationSearchRequest.getOffset(),
        validationSearchRequest.getLimit(),
        total,
        validationMapper.list(principal.getUsername(), validationSearchRequest));
  }

  @Override
  public Dataset getDataset(UUID key) {
    return get(key).getDataset();
  }

  @Override
  public List<UUID> getRunningValidations(int min) {
    Date date = Date.from(Instant.now().minus(Duration.ofMinutes(min)));
    return validationMapper.getRunningValidations(date);
  }

  @Override
  public void validateChecklistResults(UUID validationKey, ClbDatasetImport clbDatasetImport) {
    Validation validation = get(validationKey);

    if (!validation.isExecuting()) {
      throw errorMapper.apply(Validation.ErrorCode.VALIDATION_IS_NOT_EXECUTING);
    }

    if (clbDatasetImport == null) {
      log.info("CLB validation response for {}} is null", validationKey);
      validation.setStatus(Status.FAILED);
      update(validation);
      return;
    }

    if (validation.getClbDatasetKey() != clbDatasetImport.getDatasetKey()) {
      log.info(
          "CLB Dataset key {} is different from validation clb dataset key {}",
          clbDatasetImport.getDatasetKey(),
          validation.getClbDatasetKey());
      validation.setStatus(Status.FAILED);
      update(validation);
      return;
    }

    validation.setStatus(Status.RUNNING);
    update(validation);

    if (clbDatasetImport.getStatus() == ClbDatasetImport.State.failed) {
      validation.setStatus(Status.FAILED);
      update(validation);
    } else if (clbDatasetImport.getStatus() == ClbDatasetImport.State.canceled) {
      validation.setStatus(Status.ABORTED);
      update(validation);
    } else if (clbDatasetImport.getStatus() == ClbDatasetImport.State.finished) {
      try {
        List<Metrics.FileInfo> result = checklistValidator.evaluateResults(clbDatasetImport);
        log.info(
            "Validating DWCA checklist archive - finished calling checklistbank, merging results");
        result.forEach(fileInfo -> Validations.mergeFileInfo(validation, fileInfo));
        validation.setStatus(Status.FINISHED);
        update(validation);

        // send message to continue the process
        messagePublisher.send(
            checklistValidator.createNextMessage(validation.getClbValidationMessage()));
        log.info("Next message for checklist validation {} has been sent", validationKey);

      } catch (Exception e) {
        log.error("Error processing CLB validation results for {}", validationKey, e);
        validation.setStatus(Status.FAILED);
        update(validation);
      }
    } else {
      log.info(
          "Setting validation {} to FAILED since there is no valid clb validation", validationKey);
      validation.setStatus(Status.FAILED);
      update(validation);
    }
  }

  /** Persists a validation entity. */
  private Validation create(
      UUID key, DataFile dataFile, Validation.Status status, ValidationRequest validationRequest) {
    validationMapper.create(
        newValidationInstance(
            key,
            dataFile,
            getPrincipal().getUsername(), // this will validate credentials
            status,
            validationRequest.getSourceId(),
            validationRequest.getInstallationKey(),
            validationRequest.getNotificationEmail()));

    log.info("Create validation record for key {}", key);
    return validationMapper.get(key);
  }

  /** Updates the data of a validation and send MQ message. */
  private void updateAndNotifySubmitted(UUID key, DataFile dataFile) {
    try {
      Validation v =
          Optional.ofNullable(validationMapper.get(key))
              .orElse(newValidationInstance(key, dataFile, Status.QUEUED));

      Set<StepType> pipelinesSteps = getPipelineSteps(dataFile);

      // Set future steps
      v.setStatus(Status.QUEUED);
      v.setFileFormat(dataFile.getFileFormat());
      v.setFileSize(Files.size(dataFile.getFilePath()));
      v.setFile(dataFile.getSourceFileName());
      v.getMetrics().setStepTypes(StepsMapper.mapToValidationSteps(pipelinesSteps));
      v.setDataset(readEml(dataFile.getFilePath().getParent()));

      // Update DB
      updateAndGet(v);

      // Sent RabbitMQ message
      notify(key, dataFile, pipelinesSteps);
    } catch (Exception ex) {
      updateFailedValidation(key, ex.getMessage());
    }
  }

  private Dataset readEml(Path pathToArchive) {
    try {
      Optional<Path> existedPath = MetadataPath.parsePath(pathToArchive);

      if (existedPath.isEmpty()) {
        log.error("Can't find metadata eml file");
        return null;
      }

      String eml = Files.readString(existedPath.get());
      // Set the context classloader to ensure Digester can find GBIF classes
      Thread currentThread = Thread.currentThread();
      ClassLoader originalClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(getClass().getClassLoader());
        return DatasetEmlParser.build(eml.getBytes(StandardCharsets.UTF_8));
      } finally {
        currentThread.setContextClassLoader(originalClassLoader);
      }

    } catch (Exception ex) {
      log.error("Can't parse eml file and convert to Dataset. {}", ex.getMessage());
      return null;
    }
  }

  /** Updates the status of a validation process. */
  private Validation updateFailedValidation(UUID key, String errorMessage) {
    Validation validation =
        newValidationInstance(key, Validation.Status.FAILED, metricsSubmitError(errorMessage));
    return updateAndGet(validation);
  }

  /** Updates and gets the updated validation */
  private Validation updateAndGet(Validation validation) {
    validationMapper.update(validation);
    if (validation.hasFinished()) {
      log.info(
          "Validation {} finished with status {}", validation.getKey(), validation.getStatus());
      emailService.sendEmailNotification(validation);
    }
    return validationMapper.get(validation.getKey());
  }

  /** Notifies when the file is submitted. */
  @SneakyThrows
  private void notify(UUID key, DataFile dataFile, Set<StepType> pipelinesSteps) {

    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    message.setDatasetUuid(key);
    message.setAttempt(1);
    message.setExecutionId(1L);
    message.setPipelineSteps(pipelinesSteps.stream().map(Enum::name).collect(Collectors.toSet()));
    message.setFileFormat(dataFile.getFileFormat().name());

    log.info("Send the MQ message to the validator queue for key - {}", key);
    messagePublisher.send(message);
  }

  private Set<StepType> getPipelineSteps(DataFile dataFile) {
    StepType stepType;
    if (dataFile.getFileFormat() == FileFormat.DWCA
        || dataFile.getFileFormat() == FileFormat.TABULAR
        || dataFile.getFileFormat() == FileFormat.SPREADSHEET) {
      stepType = StepType.VALIDATOR_DWCA_TO_VERBATIM;
    } else if (dataFile.getFileFormat() == FileFormat.XML) {
      stepType = StepType.VALIDATOR_XML_TO_VERBATIM;
    } else {
      throw new IllegalArgumentException(
          dataFile.getFileFormat()
              + " file format is not supported, file name: "
              + dataFile.getSourceFileName());
    }

    return new LinkedHashSet<>(
        List.of(
            StepType.VALIDATOR_VALIDATE_ARCHIVE,
            stepType,
            StepType.VALIDATOR_VERBATIM_TO_IDENTIFIER,
            StepType.VALIDATOR_VERBATIM_TO_INTERPRETED,
            StepType.VALIDATOR_COLLECT_METRICS));
  }
}
