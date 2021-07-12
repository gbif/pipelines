package org.gbif.validator.ws.resource;

import static org.gbif.registry.security.UserRoles.USER_ROLE;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * Validation resource services, it allows to validates files (synchronous) and url
 * (asynchronously). Additional it provides services to list and retrieve validations statuses.
 */
@Slf4j
@RestController
@RequestMapping(value = "validation", produces = MediaType.APPLICATION_JSON_VALUE)
@Secured({USER_ROLE})
public class ValidationResource {

  private final UploadFileManager fileTransferManager;
  private final ValidationMapper validationMapper;
  private final MessagePublisher messagePublisher;

  public ValidationResource(
      UploadFileManager fileTransferManager,
      ValidationMapper validationMapper,
      MessagePublisher messagePublisher) {
    this.fileTransferManager = fileTransferManager;
    this.validationMapper = validationMapper;
    this.messagePublisher = messagePublisher;
  }

  /** Encodes an URL, specially URLs with blank spaces can be problematics. */
  private static String encode(String rawUrl) {
    try {
      String decodedURL = URLDecoder.decode(rawUrl, StandardCharsets.UTF_8.name());
      URL url = new URL(decodedURL);
      URI uri =
          new URI(
              url.getProtocol(),
              url.getUserInfo(),
              url.getHost(),
              url.getPort(),
              url.getPath(),
              url.getQuery(),
              url.getRef());
      return uri.toURL().toString();
    } catch (Exception ex) {
      log.error("Url encoding error", ex);
      throw new IllegalArgumentException(ex);
    }
  }

  /** Uploads a file and starts the validation process. */
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitFile(
      @RequestParam("file") MultipartFile file, @Autowired Principal principal) {
    UUID key = UUID.randomUUID();
    UploadFileManager.AsyncDataFileTask task =
        fileTransferManager.uploadDataFile(file, key.toString());
    return create(key, task.getStart(), principal, Validation.Status.SUBMITTED);
  }

  /** Asynchronously downloads a file from an URL and starts the validation process. */
  @PostMapping(
      path = "/url",
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitUrl(
      @RequestParam("fileUrl") String fileURL, @Autowired Principal principal)
      throws FileSizeException {
    try {
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
                updateStatus(key, Validation.Status.FAILED, err.getMessage());
              });
      return create(key, downloadResult.getDataFile(), principal, Validation.Status.DOWNLOADING);
    } catch (FileSizeException ex) {
      throw ex;
    } catch (IOException ex) {
      log.warn("Can not download file submitted", ex);
      throw new RuntimeException(ex);
    }
  }

  /** Gets the detail of Validation. */
  @GetMapping(path = "/{key}")
  public Validation get(@PathVariable UUID key) {
    return validationMapper.get(key);
  }

  /** Gets the detail of Validation. */
  @PutMapping(
      path = "/{key}",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity update(
      @PathVariable UUID key,
      @RequestBody @Valid @NotNull Validation validation,
      @Autowired Principal principal) {
    if (!key.equals(validation.getKey())) {
      return ResponseEntity.badRequest().body("Wrong validation key for this url");
    }

    Validation existingValidation = validationMapper.get(key);
    if (existingValidation == null) {
      return ResponseEntity.notFound().build();
    }
    if (!canUpdate(existingValidation, principal)) {
      return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
    } else {
      update(validation);
      return ResponseEntity.ok().build();
    }
  }

  private boolean canUpdate(Validation validation, Principal principal) {
    return SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream()
            .anyMatch(a -> a.getAuthority().equals(UserRoles.ADMIN_ROLE))
        || validation.getUsername().equals(principal.getName());
  }

  /** Lists the validations of an user. */
  @GetMapping
  public PagingResponse<Validation> list(Pageable page, @Autowired Principal principal) {
    page = page == null ? new PagingRequest() : page;
    long total = validationMapper.count(principal.getName());
    return new PagingResponse<>(
        page.getOffset(), page.getLimit(), total, validationMapper.list(page, principal.getName()));
  }

  /** Persists an validation entity. */
  private Validation create(
      UUID key, DataFile dataFile, Principal principal, Validation.Status status) {
    Validation validation =
        Validation.builder()
            .key(key)
            .fileFormat(dataFile.getFileFormat())
            .status(status)
            .file(dataFile.getFilePath().toString())
            .fileSize(dataFile.getSize())
            .username(principal.getName())
            .build();
    validationMapper.create(validation);
    validation = validationMapper.get(key);
    if (Validation.Status.SUBMITTED == validation.getStatus()) {
      notify(validation);
    }
    return validation;
  }

  @SneakyThrows
  private void notify(Validation validation) {
    PipelinesArchiveValidatorMessage message = new PipelinesArchiveValidatorMessage();
    message.setValidator(true);
    message.setDatasetUuid(validation.getKey());
    messagePublisher.send(message);
  }

  /** Updates the data of a validation. */
  private Validation update(UUID key, DataFile dataFile, Validation.Status status) {
    Validation validation =
        Validation.builder()
            .key(key)
            .fileFormat(dataFile.getFileFormat())
            .status(status)
            .file(dataFile.getFilePath().toString())
            .fileSize(dataFile.getSize())
            .build();
    update(validation);
    validation = validationMapper.get(key);
    return validation;
  }

  /** Updates and notifies the update. */
  private void update(Validation validation) {
    validationMapper.update(validation);
    notify(validation);
  }

  /** Updates the status of a validation process. */
  private Validation updateStatus(UUID key, Validation.Status status, String result) {
    Validation validation = Validation.builder().key(key).status(status).result(result).build();
    validationMapper.update(validation);
    return validationMapper.get(key);
  }
}
