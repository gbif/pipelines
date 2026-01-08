package org.gbif.validator.ws.resource;

import static org.gbif.ws.security.UserRoles.ADMIN_ROLE;
import static org.gbif.ws.security.UserRoles.APP_ROLE;
import static org.gbif.ws.security.UserRoles.IPT_ROLE;
import static org.gbif.ws.security.UserRoles.USER_ROLE;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.service.ErrorMapper;
import org.gbif.validator.service.ValidationService;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.DeleteMapping;
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
 * Validation resource services, it allows validating files (synchronous) and url (asynchronously).
 * Aditional it provides services to list and retrieve validations statuses.
 */
@Slf4j
@RestController
@RequestMapping(value = "validation", produces = MediaType.APPLICATION_JSON_VALUE)
@Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
@RequiredArgsConstructor
public class ValidationResource {

  private final ValidationService<MultipartFile> validationService;

  private final ErrorMapper errorMapper;

  /** Uploads a file and starts the validation process. */
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitFile(
      @RequestParam("file") MultipartFile file, @Valid ValidationRequest validationRequest) {
    return validationService.validateFile(file, validationRequest);
  }

  /** Asynchronously downloads a file from a URL and starts the validation process. */
  @PostMapping(
      path = "/url",
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitUrl(
      @RequestParam("fileUrl") String fileURL, @Valid ValidationRequest validationRequest) {
    return validationService.validateFileFromUrl(fileURL, validationRequest);
  }

  /** Gets the detail of Validation. */
  @GetMapping(path = "/{key}")
  public Validation get(@PathVariable("key") UUID key) {
    return validationService.get(key);
  }

  /** Cancels a Validation. */
  @PutMapping(path = "/{key}/cancel")
  public Validation cancel(@PathVariable("key") UUID key) {
    return validationService.cancel(key);
  }

  /** Deletes a Validation. */
  @DeleteMapping(path = "/{key}")
  public void delete(@PathVariable("key") UUID key) {
    validationService.delete(key);
  }

  /** Gets the detail of Validation. */
  @PutMapping(
      path = "/{key}",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  public Validation update(
      @PathVariable("key") UUID key, @RequestBody @Valid @NotNull Validation validation) {
    if (!key.equals(validation.getKey())) {
      throw errorMapper.apply(Validation.ErrorCode.WRONG_KEY_IN_REQUEST);
    }
    return validationService.update(validation);
  }

  /** Get EML data */
  @GetMapping(path = "/{key}/eml")
  public Dataset getEml(@PathVariable("key") UUID key) {
    return validationService.getDataset(key);
  }

  /** Lists the validations of a user. */
  @GetMapping
  public PagingResponse<Validation> list(@Valid ValidationSearchRequest validationSearchRequest) {
    return validationService.list(validationSearchRequest);
  }

  /** Returns list of validations running for more than specified time in min. */
  @Secured({ADMIN_ROLE})
  @GetMapping(path = "/running")
  public List<UUID> getRunningValidations(@RequestParam("min") int min) {
    return validationService.getRunningValidations(min);
  }
}
