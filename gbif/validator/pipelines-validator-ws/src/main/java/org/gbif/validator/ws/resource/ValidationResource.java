package org.gbif.validator.ws.resource;

import static org.gbif.ws.security.UserRoles.ADMIN_ROLE;
import static org.gbif.ws.security.UserRoles.APP_ROLE;
import static org.gbif.ws.security.UserRoles.IPT_ROLE;
import static org.gbif.ws.security.UserRoles.USER_ROLE;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.validator.api.ClbDatasetImport;
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
 * Additional it provides services to list and retrieve validations statuses.
 */
@Tag(
    name = "Validation",
    description =
        "Operations for submitting files/URLs for validation and managing validation jobs")
@Slf4j
@RestController
@RequestMapping(value = "validation", produces = MediaType.APPLICATION_JSON_VALUE)
@RequiredArgsConstructor
public class ValidationResource {

  private final ValidationService<MultipartFile> validationService;

  private final ErrorMapper errorMapper;

  /** Uploads a file and starts the validation process. */
  @Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
  @Operation(
      summary = "Upload a file and start validation",
      description = "Uploads a file and starts synchronous validation.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Validation started",
        content = @Content(schema = @Schema(implementation = Validation.class))),
    @ApiResponse(responseCode = "400", description = "Invalid request")
  })
  @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitFile(
      @Parameter(description = "File to validate") @RequestParam("file") MultipartFile file,
      @Valid ValidationRequest validationRequest) {
    return validationService.validateFile(file, validationRequest);
  }

  /** Asynchronously downloads a file from a URL and starts the validation process. */
  @Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
  @Operation(
      summary = "Submit a URL for validation",
      description = "Asynchronously downloads a file from the provided URL and starts validation.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Validation job accepted",
        content = @Content(schema = @Schema(implementation = Validation.class))),
    @ApiResponse(responseCode = "400", description = "Invalid URL or request")
  })
  @PostMapping(
      path = "/url",
      consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
  public Validation submitUrl(
      @Parameter(description = "URL of the file to validate") @RequestParam("fileUrl")
          String fileURL,
      @Valid ValidationRequest validationRequest) {
    return validationService.validateFileFromUrl(fileURL, validationRequest);
  }

  /** Gets the detail of Validation. */
  @Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
  @Operation(
      summary = "Get validation details",
      description = "Retrieve details for a specific validation job.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Validation found",
        content = @Content(schema = @Schema(implementation = Validation.class))),
    @ApiResponse(responseCode = "404", description = "Validation not found")
  })
  @GetMapping(path = "/{key}")
  public Validation get(@Parameter(description = "Validation key") @PathVariable("key") UUID key) {
    return validationService.get(key);
  }

  /** Cancels a Validation. */
  @Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
  @Operation(
      summary = "Cancel validation",
      description = "Cancel a running or queued validation job.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Validation cancelled",
        content = @Content(schema = @Schema(implementation = Validation.class))),
    @ApiResponse(responseCode = "404", description = "Validation not found")
  })
  @PutMapping(path = "/{key}/cancel")
  public Validation cancel(
      @Parameter(description = "Validation key") @PathVariable("key") UUID key) {
    return validationService.cancel(key);
  }

  /** Deletes a Validation. */
  @Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
  @Operation(
      summary = "Delete validation",
      description = "Delete a validation job and its results.")
  @ApiResponses({
    @ApiResponse(responseCode = "204", description = "Validation deleted"),
    @ApiResponse(responseCode = "404", description = "Validation not found")
  })
  @DeleteMapping(path = "/{key}")
  public void delete(@Parameter(description = "Validation key") @PathVariable("key") UUID key) {
    validationService.delete(key);
  }

  /** Updates the detail of Validation. */
  @Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
  @Operation(summary = "Update validation", description = "Update the details of a validation job.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Validation updated",
        content = @Content(schema = @Schema(implementation = Validation.class))),
    @ApiResponse(responseCode = "400", description = "Invalid request"),
    @ApiResponse(responseCode = "404", description = "Validation not found")
  })
  @PutMapping(
      path = "/{key}",
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  public Validation update(
      @Parameter(description = "Validation key") @PathVariable("key") UUID key,
      @RequestBody @Valid @NotNull Validation validation) {
    if (!key.equals(validation.getKey())) {
      throw errorMapper.apply(Validation.ErrorCode.WRONG_KEY_IN_REQUEST);
    }
    return validationService.update(validation);
  }

  /** Get EML data */
  @Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
  @Operation(
      summary = "Get EML data",
      description = "Return EML (metadata) for a validation's dataset.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "EML returned",
        content = @Content(schema = @Schema(implementation = Dataset.class))),
    @ApiResponse(responseCode = "404", description = "Dataset not found")
  })
  @GetMapping(path = "/{key}/eml")
  public Dataset getEml(@Parameter(description = "Validation key") @PathVariable("key") UUID key) {
    return validationService.getDataset(key);
  }

  /** Lists the validations of a user. */
  @Secured({USER_ROLE, APP_ROLE, IPT_ROLE, ADMIN_ROLE})
  @Operation(
      summary = "List validations",
      description = "List validations for the current user with paging and filters.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "List returned",
        content = @Content(schema = @Schema(implementation = PagingResponse.class)))
  })
  @GetMapping
  public PagingResponse<Validation> list(@Valid ValidationSearchRequest validationSearchRequest) {
    return validationService.list(validationSearchRequest);
  }

  /** Returns list of validations running for more than specified time in min. */
  @Secured({ADMIN_ROLE})
  @Operation(
      summary = "Get long-running validations",
      description =
          "Returns list of validation keys running longer than the specified minutes (admin only).")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "List returned",
        content = @Content(schema = @Schema(implementation = UUID.class)))
  })
  @GetMapping(path = "/running")
  public List<UUID> getRunningValidations(
      @Parameter(description = "Minimum running time in minutes") @RequestParam("min") int min) {
    return validationService.getRunningValidations(min);
  }

  @Hidden
  @PostMapping(
      path = "/clbValidationCallback/{validationKey}",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public void clbValidationCallback(
      @PathVariable("validationKey") UUID validationKey,
      @RequestBody String clbDatasetImport) {
    log.info(
        "ClbValidationCallback import received for validation {}: {} ",
        validationKey,
        clbDatasetImport);
//    validationService.validateChecklistResults(validationKey, clbDatasetImport);
  }
}
