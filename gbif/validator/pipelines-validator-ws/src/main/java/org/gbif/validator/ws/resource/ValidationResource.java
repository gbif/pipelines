package org.gbif.validator.ws.resource;

import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.service.ErrorMapper;
import org.gbif.validator.service.ValidationService;
import org.springframework.http.MediaType;
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

import static org.gbif.registry.security.UserRoles.IPT_ROLE;
import static org.gbif.registry.security.UserRoles.APP_ROLE;
import static org.gbif.registry.security.UserRoles.USER_ROLE;

/**
 * Validation resource services, it allows validating files (synchronous) and url (asynchronously).
 * Additional it provides services to list and retrieve validations statuses.
 */
@Slf4j
@RestController
@RequestMapping(value = "validation", produces = MediaType.APPLICATION_JSON_VALUE)
@Secured({USER_ROLE, APP_ROLE, IPT_ROLE})
@RequiredArgsConstructor
@Validated
public class ValidationResource {

  private final ValidationService<MultipartFile> validationService;

  private final ErrorMapper errorMapper;

  /** Uploads a file and starts the validation process. */
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitFile(
      @RequestParam("file") MultipartFile file, @Valid ValidationRequest validationRequest) {
    return validationService.validateFile(file, validationRequest);
  }

  /** Asynchronously downloads a file from an URL and starts the validation process. */
  @PostMapping(
      path = "/url",
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitUrl(
      @RequestParam("fileUrl") String fileURL, @Valid ValidationRequest validationRequest) {
    return validationService.validateFileFromUrl(fileURL, validationRequest);
  }

  /** Gets the detail of Validation. */
  @GetMapping(path = "/{key}")
  public Validation get(@PathVariable UUID key) {
    return validationService.get(key);
  }

  /** Cancels a Validation. */
  @PutMapping(path = "/{key}/cancel")
  public Validation cancel(@PathVariable UUID key) {
    return validationService.cancel(key);
  }

  /** Gets the detail of Validation. */
  @PutMapping(
      path = "/{key}",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  public Validation update(
      @PathVariable UUID key, @RequestBody @Valid @NotNull Validation validation) {
    if (!key.equals(validation.getKey())) {
      throw errorMapper.apply(Validation.ErrorCode.WRONG_KEY_IN_REQUEST);
    }
    return validationService.update(validation);
  }

  /** Lists the validations of a user. */
  @GetMapping
  public PagingResponse<Validation> list(@Valid ValidationSearchRequest validationSearchRequest) {
    return validationService.list(validationSearchRequest);
  }
}
