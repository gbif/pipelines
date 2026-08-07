package org.gbif.validator.ws.resource;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.EvaluationType;
import org.gbif.validator.api.FileFormat;
import org.gbif.validator.api.Validation;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Resource to list the vocabularies/enums used by the validation services. */
@OpenAPIDefinition(
    info =
        @Info(
            title = "Validator API",
            version = "v1",
            description =
                "This API allows to validate datasets, EMLs and sequences. It may be of interest to those coding "
                    + "against the API, and can be found in the "
                    + "[validator-ws-client](https://github.com/gbif/pipelines/tree/master/gbif/validator/validator-ws-client) project.",
            termsOfService = "https://www.gbif.org/terms"),
    servers = {
      @Server(url = "https://api.gbif.org/v1/", description = "Production"),
      @Server(url = "https://api.gbif-test.org/v1/", description = "User testing")
    })
@Tag(
    name = "Enumerations",
    description = "Listing of enums and vocabularies used by validation services")
@RestController
@RequestMapping(value = "validation/enumeration", produces = MediaType.APPLICATION_JSON_VALUE)
public class EnumerationResource {

  private static final Set<String> INVENTORY =
      Stream.of(
              DwcFileType.class.getSimpleName(),
              EvaluationCategory.class.getSimpleName(),
              EvaluationType.class.getSimpleName(),
              FileFormat.class.getSimpleName(),
              DwcFileType.class.getSimpleName(),
              "ValidationStatus") // Validation.Status
          .collect(Collectors.toSet());

  @Operation(summary = "Inventory of enums", description = "Return available enum/vocabulary types")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Inventory returned",
        content = @Content(schema = @Schema(implementation = String.class)))
  })
  @GetMapping
  public Set<String> inventory() {
    return INVENTORY;
  }

  @Operation(
      summary = "List evaluation categories",
      description = "Return all evaluation categories")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Categories returned",
        content = @Content(schema = @Schema(implementation = EvaluationCategory.class)))
  })
  @GetMapping(value = "EvaluationCategory")
  public EvaluationCategory[] evaluationCategories() {
    return EvaluationCategory.values();
  }

  @Operation(
      summary = "Evaluation types by category",
      description = "Return evaluation types for a given category")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Evaluation types returned",
        content = @Content(schema = @Schema(implementation = EvaluationType.class))),
    @ApiResponse(responseCode = "404", description = "Category not found")
  })
  @GetMapping(value = "EvaluationCategory/{evaluationCategory}")
  public List<EvaluationType> categoryEvaluationTypes(
      @Parameter(description = "Evaluation category") @PathVariable("evaluationCategory")
          EvaluationCategory evaluationCategory) {
    return listEvaluationTypes(evaluationCategory);
  }

  @Operation(
      summary = "List evaluation types",
      description = "Return evaluation types, optionally filtered by category")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Evaluation types returned",
        content = @Content(schema = @Schema(implementation = EvaluationType.class)))
  })
  @GetMapping(value = "EvaluationType")
  public List<EvaluationType> evaluationTypes(
      @Parameter(description = "Optional evaluation category to filter by")
          @RequestParam(value = "evaluationCategory", required = false)
          EvaluationCategory evaluationCategory) {
    return listEvaluationTypes(evaluationCategory);
  }

  private static List<EvaluationType> listEvaluationTypes(EvaluationCategory evaluationCategory) {
    return Stream.of(EvaluationType.values())
        .filter(et -> evaluationCategory == null || evaluationCategory == et.getCategory())
        .collect(Collectors.toList());
  }

  @Operation(summary = "List DwcFileType values", description = "Return Darwin Core file types")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "DwcFileType values returned",
        content = @Content(schema = @Schema(implementation = DwcFileType.class)))
  })
  @GetMapping(value = "DwcFileType")
  public DwcFileType[] dwcFileTypes() {
    return DwcFileType.values();
  }

  @Operation(summary = "List file formats", description = "Return supported file formats")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "File formats returned",
        content = @Content(schema = @Schema(implementation = FileFormat.class)))
  })
  @GetMapping(value = "FileFormat")
  public FileFormat[] fileFormats() {
    return FileFormat.values();
  }

  @Operation(
      summary = "List validation statuses",
      description = "Return possible validation statuses")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Validation statuses returned",
        content = @Content(schema = @Schema(implementation = Validation.Status.class)))
  })
  @GetMapping(value = "ValidationStatus")
  public Validation.Status[] validationStatuses() {
    return Validation.Status.values();
  }
}
