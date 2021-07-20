package org.gbif.validator.service;

import static org.gbif.validator.service.EncodingUtil.encode;
import static org.gbif.validator.service.ValidationFactory.metricsFromError;
import static org.gbif.validator.service.ValidationFactory.newValidationInstance;

import io.vavr.control.Either;
import java.io.IOException;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.registry.security.UserRoles;
import org.gbif.validator.api.Validation;
import org.gbif.validator.persistence.mapper.ValidationMapper;
import org.gbif.validator.ws.file.DataFile;
import org.gbif.validator.ws.file.FileSizeException;
import org.gbif.validator.ws.file.UploadFileManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

@Service
@Slf4j
public class ValidationService {

  private final UploadFileManager fileTransferManager;

  private final ValidationMapper validationMapper;

  private final MessagePublisher messagePublisher;

  private final int maxRunningValidationPerUser;

  public ValidationService(
      UploadFileManager fileTransferManager,
      ValidationMapper validationMapper,
      MessagePublisher messagePublisher,
      @Value("${maxRunningValidationPerUser}") int maxRunningValidationPerUser) {
    this.fileTransferManager = fileTransferManager;
    this.validationMapper = validationMapper;
    this.messagePublisher = messagePublisher;
    this.maxRunningValidationPerUser = maxRunningValidationPerUser;
  }

  @Builder
  @Data
  @AllArgsConstructor(staticName = "of")
  @RequiredArgsConstructor(staticName = "of")
  public static class Error {
    public enum Code {
      MAX_RUNNING_VALIDATIONS,
      MAX_FILE_SIZE_VIOLATION,
      AUTHORIZATION_ERROR,
      NOT_FOUND,
      IO_ERROR,
      VALIDATION_IS_NOT_EXECUTING;
    }

    private final Code code;
    private Throwable error;
  }

  /** Asserts the user has not reached the maximum number of executing validations. */
  public Optional<Error> assertRunningValidation(String userName) {
    if (validationMapper.count(userName, Validation.executingStatuses())
        >= maxRunningValidationPerUser) {
      return Optional.of(Error.of(Error.Code.MAX_RUNNING_VALIDATIONS));
    }
    return Optional.empty();
  }

  public Either<Error, Validation> submitFile(MultipartFile file, Principal principal) {
    Optional<Error> error = assertRunningValidation(principal.getName());
    if (error.isPresent()) {
      return Either.left(error.get());
    }
    UUID key = UUID.randomUUID();
    UploadFileManager.AsyncDataFileTask task =
        fileTransferManager.uploadDataFile(file, key.toString());
    return Either.right(
        create(key, task.getStart(), principal.getName(), Validation.Status.SUBMITTED));
  }

  public Either<Error, Validation> validateFileFromUrl(String fileURL, Principal principal) {
    try {
      Optional<Error> error = assertRunningValidation(principal.getName());
      if (error.isPresent()) {
        return Either.left(error.get());
      }
      UUID key = UUID.randomUUID();
      String encodedFileURL = encode(fileURL);
      // this should also become asynchronous at some point
      UploadFileManager.AsyncDownloadResult downloadResult =
          fileTransferManager.downloadDataFile(
              encodedFileURL,
              key.toString(),
              resultDataFile -> update(key, resultDataFile, Validation.Status.SUBMITTED),
              err -> {
                log.error("Error processing file", err);
                updateFailedValidation(key, err.getMessage());
              });
      return Either.right(
          create(
              key,
              downloadResult.getDataFile(),
              principal.getName(),
              Validation.Status.DOWNLOADING));
    } catch (FileSizeException ex) {
      log.error("File limit error", ex);
      return Either.left(Error.of(Error.Code.MAX_FILE_SIZE_VIOLATION, ex));
    } catch (IOException ex) {
      log.error("Can not download file submitted", ex);
      return Either.left(Error.of(Error.Code.IO_ERROR, ex));
    }
  }

  public Either<Error, Validation> get(UUID key) {
    Validation validation = validationMapper.get(key);
    return validation != null
        ? Either.right(validation)
        : Either.left(Error.of(Error.Code.NOT_FOUND));
  }

  public Either<Error, Validation> update(Validation validation, Principal principal) {
    Either<Error, Validation> existingValidation = get(validation.getKey());
    if (existingValidation.isLeft()) {
      return existingValidation;
    }
    if (!canUpdate(existingValidation.get(), principal)) {
      return Either.left(Error.of(Error.Code.AUTHORIZATION_ERROR));
    }
    return Either.right(updateAndGet(validation));
  }

  public Either<Error, Validation> cancel(UUID key, Principal principal) {
    Either<Error, Validation> result = get(key);
    if (result.isLeft()) {
      return result;
    }
    Validation validation = result.get();
    if (!canUpdate(validation, principal)) {
      return Either.left(Error.of(Error.Code.AUTHORIZATION_ERROR));
    }
    validation.setStatus(Validation.Status.ABORTED);
    return Either.right(updateAndGet(validation));
  }

  public PagingResponse<Validation> list(
      Pageable page, Set<Validation.Status> status, Principal principal) {
    page = page == null ? new PagingRequest() : page;
    long total = validationMapper.count(principal.getName(), status);
    return new PagingResponse<>(
        page.getOffset(),
        page.getLimit(),
        total,
        validationMapper.list(page, principal.getName(), status));
  }

  /** Can the authenticated user update the validation object. */
  private boolean canUpdate(Validation validation, Principal principal) {
    return SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream()
            .anyMatch(a -> a.getAuthority().equals(UserRoles.ADMIN_ROLE))
        || validation.getUsername().equals(principal.getName());
  }

  /** Persists an validation entity. */
  private Validation create(
      UUID key, DataFile dataFile, String userName, Validation.Status status) {
    validationMapper.create(newValidationInstance(key, dataFile, userName, status));
    Validation validation = validationMapper.get(key);
    // Downloading is not notify
    if (Validation.Status.DOWNLOADING != validation.getStatus()) {
      notify(validation);
    }
    return validation;
  }

  /** Updates the data of a validation. */
  private Validation update(UUID key, DataFile dataFile, Validation.Status status) {
    Validation validation = updateAndGet(newValidationInstance(key, dataFile, status));
    notify(validation);
    return validation;
  }

  /** Updates the status of a validation process. */
  private Validation updateFailedValidation(UUID key, String errorMessage) {
    Validation validation =
        newValidationInstance(key, Validation.Status.FAILED, metricsFromError(errorMessage));
    return updateAndGet(validation);
  }

  private Validation updateAndGet(Validation validation) {
    validationMapper.update(validation);
    return validationMapper.get(validation.getKey());
  }

  @SneakyThrows
  private void notify(Validation validation) {
    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    message.setValidator(true);
    message.setDatasetUuid(validation.getKey());
    messagePublisher.send(message);
  }
}
