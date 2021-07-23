package org.gbif.validator.ws.resource;

import static org.gbif.registry.security.UserRoles.USER_ROLE;

import java.security.Principal;
import java.util.Set;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.mail.validator.ValidatorEmailService;
import org.gbif.validator.api.Validation;
import org.gbif.validator.service.ErrorMapper;
import org.gbif.validator.service.ValidationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
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
@RequiredArgsConstructor
public class ValidationResource {

  private final ValidationService<MultipartFile> validationService;
  private final ErrorMapper errorMapper;
  private final ValidatorEmailService emailService;

  /** Uploads a file and starts the validation process. */
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitFile(
      @RequestParam("file") MultipartFile file, @Autowired Principal principal) {
    return errorMapper.getOrElseThrow(validationService.validateFile(file, principal));
  }

  /** Asynchronously downloads a file from an URL and starts the validation process. */
  @PostMapping(
      path = "/url",
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitUrl(
      @RequestParam("fileUrl") String fileURL, @Autowired Principal principal) {
    return errorMapper.getOrElseThrow(validationService.validateFileFromUrl(fileURL, principal));
  }

  /** Gets the detail of Validation. */
  @GetMapping(path = "/{key}")
  public Validation get(@PathVariable UUID key) {
    return errorMapper.getOrElseThrow(validationService.get(key));
  }

  /** Cancels a Validation. */
  @PutMapping(path = "/{key}/cancel")
  public void cancel(@PathVariable UUID key, @Autowired Principal principal) {
    errorMapper.getOrElseThrow(validationService.cancel(key, principal));
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
    return errorMapper.getOrElseThrow(
        validationService.update(validation, principal).map(v -> ResponseEntity.ok().build()));
  }

  /** Lists the validations of an user. */
  @GetMapping
  public PagingResponse<Validation> list(
      Pageable page,
      @RequestParam(value = "status", required = false) Set<Validation.Status> status,
      @Autowired Principal principal) {
    return validationService.list(page, status, principal);
  }
}
