package org.gbif.validator.ws.resource;

import static org.gbif.registry.security.UserRoles.USER_ROLE;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.UUID;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;
import org.gbif.validator.persistence.mapper.ValidationMapper;
import org.gbif.validator.ws.file.DataFile;
import org.gbif.validator.ws.file.FileSizeException;
import org.gbif.validator.ws.file.UploadFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * Validation resource services, it allows to validates files (synchronous) and url
 * (asynchronously). Additional it provides services to list and retrieve validations statuses.
 */
@RestController
@RequestMapping(value = "validation", produces = MediaType.APPLICATION_JSON_VALUE)
@Secured({USER_ROLE})
public class ValidationResource {

  private static final Logger LOG = LoggerFactory.getLogger(ValidationResource.class);

  private final UploadFileManager fileTransferManager;
  private final ValidationMapper validationMapper;

  public ValidationResource(
      UploadFileManager fileTransferManager, ValidationMapper validationMapper) {
    this.fileTransferManager = fileTransferManager;
    this.validationMapper = validationMapper;
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
      LOG.error("Url encoding error", ex);
      throw new IllegalArgumentException(ex);
    }
  }

  /** Uploads a file and starts the validation process. */
  @PostMapping(
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE},
      produces = {MediaType.APPLICATION_JSON_VALUE})
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
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE},
      produces = {MediaType.APPLICATION_JSON_VALUE})
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
                LOG.error("Error processing file", err);
                updateStatus(key, Validation.Status.SUBMITTED, err.getMessage());
              });
      return create(key, downloadResult.getDataFile(), principal, Validation.Status.DOWNLOADING);
    } catch (FileSizeException ex) {
      throw ex;
    } catch (IOException ex) {
      LOG.warn("Can not download file submitted", ex);
      throw new RuntimeException(ex);
    }
  }

  /** Lists the validations of an user. */
  @GetMapping(produces = {MediaType.APPLICATION_JSON_VALUE})
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
    return validationMapper.get(key);
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
    validationMapper.update(validation);
    return validationMapper.get(key);
  }

  /** Updates the status of a validation process. */
  private Validation updateStatus(UUID key, Validation.Status status, String result) {
    Validation validation = Validation.builder().key(key).status(status).result(result).build();
    validationMapper.update(validation);
    return validationMapper.get(key);
  }
}
