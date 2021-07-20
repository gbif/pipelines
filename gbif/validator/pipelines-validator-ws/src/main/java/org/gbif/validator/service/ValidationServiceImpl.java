package org.gbif.validator.service;

import static org.gbif.validator.service.EncodingUtil.encode;
import static org.gbif.validator.service.ValidationFactory.metricsFromError;
import static org.gbif.validator.service.ValidationFactory.newValidationInstance;

import io.vavr.control.Either;
import io.vavr.control.Option;
import java.io.IOException;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
@RequiredArgsConstructor
public class ValidationServiceImpl implements ValidationService<MultipartFile> {

  private final UploadFileManager fileTransferManager;

  private final ValidationMapper validationMapper;

  private final MessagePublisher messagePublisher;

  @Value("${maxRunningValidationPerUser}")
  private final int maxRunningValidationPerUser;

  /** Asserts the user has not reached the maximum number of executing validations. */
  @Override
  public Optional<Validation.Error> reachedMaxRunningValidation(String userName) {
    if (validationMapper.count(userName, Validation.executingStatuses())
        >= maxRunningValidationPerUser) {
      return Optional.of(Validation.Error.of(Validation.Error.Code.MAX_RUNNING_VALIDATIONS));
    }
    return Optional.empty();
  }

  @Override
  public Either<Validation.Error, Validation> submitFile(MultipartFile file, Principal principal) {
    Optional<Validation.Error> error = reachedMaxRunningValidation(principal.getName());
    if (error.isPresent()) {
      return Either.left(error.get());
    }
    UUID key = UUID.randomUUID();
    UploadFileManager.AsyncDataFileTask task =
        fileTransferManager.uploadDataFile(file, key.toString());
    fileTransferManager.uploadDataFile(file, key.toString());
    return Either.right(
        create(key, task.getStart(), principal.getName(), Validation.Status.SUBMITTED));
  }

  @Override
  public Either<Validation.Error, Validation> validateFileFromUrl(
      String fileURL, Principal principal) {
    try {
      Optional<Validation.Error> error = reachedMaxRunningValidation(principal.getName());
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
      return Either.left(Validation.Error.of(Validation.Error.Code.MAX_FILE_SIZE_VIOLATION, ex));
    } catch (IOException ex) {
      log.error("Can not download file submitted", ex);
      return Either.left(Validation.Error.of(Validation.Error.Code.IO_ERROR, ex));
    }
  }

  /** Gets a validation by its key, if exists */
  @Override
  public Either<Validation.Error, Validation> get(UUID key) {
    return Option.of(validationMapper.get(key))
        .toEither(Validation.Error.of(Validation.Error.Code.NOT_FOUND));
  }

  /** Updates validation data. */
  @Override
  public Either<Validation.Error, Validation> update(Validation validation, Principal principal) {
    return get(validation.getKey())
        .filterOrElse(
            v -> canUpdate(v, principal),
            v -> Validation.Error.of(Validation.Error.Code.AUTHORIZATION_ERROR))
        .map(v -> updateAndGet(validation));
  }

  /** Cancels a running validation. */
  @Override
  public Either<Validation.Error, Validation> cancel(UUID key, Principal principal) {
    return get(key)
        .filterOrElse(
            v -> canUpdate(v, principal),
            v -> Validation.Error.of(Validation.Error.Code.AUTHORIZATION_ERROR))
        .filterOrElse(
            v -> v.isExecuting(),
            v -> Validation.Error.of(Validation.Error.Code.VALIDATION_IS_NOT_EXECUTING))
        .map(
            v -> {
              v.setStatus(Validation.Status.ABORTED);
              return updateAndGet(v);
            });
  }

  /** Paged result of validations of a an user. */
  @Override
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

  /** Updates and gets the updated validation */
  private Validation updateAndGet(Validation validation) {
    validationMapper.update(validation);
    return validationMapper.get(validation.getKey());
  }

  /** Notifies a change or creation of a Validation. */
  @SneakyThrows
  private void notify(Validation validation) {
    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    message.setValidator(true);
    message.setDatasetUuid(validation.getKey());
    messagePublisher.send(message);
  }
}
