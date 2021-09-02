package org.gbif.validator.ws.resource;

import static org.gbif.registry.security.UserRoles.USER_ROLE;

import java.security.Principal;
import java.util.Set;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.Email;
import javax.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;
import org.gbif.validator.service.ErrorMapper;
import org.gbif.validator.service.ValidationService;
import org.gbif.validator.ws.security.ValidateInstallationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

/**
 * Validation resource services, it allows validating files (synchronous) and url (asynchronously).
 * Additional it provides services to list and retrieve validations statuses.
 */
@Slf4j
@RestController
@RequestMapping(value = "validation", produces = MediaType.APPLICATION_JSON_VALUE)
@Secured({USER_ROLE})
@RequiredArgsConstructor
@Validated
public class ValidationResource {

  private final ValidationService<MultipartFile> validationService;
  private final ErrorMapper errorMapper;
  private final ValidateInstallationService validateInstallationService;

  private void validateRequest(
      UUID installationKey, Set<String> notificationEmails, HttpServletRequest httpServletRequest) {
    validateInstallationService.validateInstallationAccess(installationKey, httpServletRequest);
    if (installationKey != null) {
      if (notificationEmails == null || notificationEmails.isEmpty()) {
        throw new ResponseStatusException(
            HttpStatus.BAD_REQUEST,
            "Notification emails must be provided when installation key is present");
      }
    }
  }

  /** Uploads a file and starts the validation process. */
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitFile(
      @RequestParam("file") MultipartFile file,
      @RequestParam(value = "sourceId", required = false) String resourceId,
      @RequestParam(value = "installationKey", required = false) UUID installationKey,
      @RequestParam(value = "notificationEmail", required = false)
          Set<@Email String> notificationEmails,
      @Autowired Principal principal,
      @Autowired HttpServletRequest httpServletRequest) {
    validateRequest(installationKey, notificationEmails, httpServletRequest);
    return errorMapper.getOrElseThrow(
        validationService.validateFile(
            file, principal, resourceId, installationKey, notificationEmails));
  }

  /** Asynchronously downloads a file from an URL and starts the validation process. */
  @PostMapping(
      path = "/url",
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitUrl(
      @RequestParam("fileUrl") String fileURL,
      @RequestParam(value = "sourceId", required = false) String resourceId,
      @RequestParam(value = "installationKey", required = false) UUID installationKey,
      @RequestParam(value = "notificationEmail", required = false)
          Set<@Email String> notificationEmails,
      @Autowired Principal principal,
      @Autowired HttpServletRequest httpServletRequest) {
    validateRequest(installationKey, notificationEmails, httpServletRequest);
    return errorMapper.getOrElseThrow(
        validationService.validateFileFromUrl(
            fileURL, principal, resourceId, installationKey, notificationEmails));
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
  public ResponseEntity<?> update(
      @PathVariable UUID key,
      @RequestBody @Valid @NotNull Validation validation,
      @Autowired Principal principal,
      @Autowired HttpServletRequest httpServletRequest) {
    if (!key.equals(validation.getKey())) {
      return ResponseEntity.badRequest().body("Wrong validation key for this url");
    }
    validateInstallationService.validateInstallationAccess(
        validation.getInstallationKey(), httpServletRequest);
    return errorMapper.getOrElseThrow(
        validationService.update(validation, principal).map(v -> ResponseEntity.ok().build()));
  }

  /** Lists the validations of a user. */
  @GetMapping
  public PagingResponse<Validation> list(
      Pageable page,
      @RequestParam(value = "status", required = false) Set<Validation.Status> status,
      @RequestParam(value = "installationKey", required = false) UUID installationKey,
      @RequestParam(value = "sourceId", required = false) String sourceId,
      @Autowired Principal principal) {
    return validationService.list(page, status, installationKey, sourceId, principal);
  }
}
