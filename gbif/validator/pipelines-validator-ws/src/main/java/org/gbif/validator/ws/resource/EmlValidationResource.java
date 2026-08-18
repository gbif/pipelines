package org.gbif.validator.ws.resource;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.ws.config.ValidatorWsConfiguration;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "EML Validation", description = "EML validation operations")
@RestController
@RequestMapping(value = "validation/eml", produces = MediaType.APPLICATION_JSON_VALUE)
@RequiredArgsConstructor
public class EmlValidationResource {

  private final ValidatorWsConfiguration.XmlSchemaLocations schemaLocations;

  private final SchemaValidatorFactory schemaValidatorFactory;

  /** Validates an EML document. */
  @Operation(
      summary = "Validate EML document",
      description = "Validates an EML XML document against supported schemas.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Validation issues returned",
        content = @Content(schema = @Schema(implementation = IssueInfo.class))),
    @ApiResponse(responseCode = "400", description = "Invalid EML document")
  })
  @PostMapping(
      consumes = {MediaType.APPLICATION_XML_VALUE},
      produces = {MediaType.APPLICATION_JSON_VALUE})
  @SneakyThrows
  public List<IssueInfo> validateEml(
      @io.swagger.v3.oas.annotations.parameters.RequestBody(
              description = "EML document as XML bytes")
          @RequestBody
          byte[] document) {
    String xmlDoc = new String(document, StandardCharsets.UTF_8);
    return schemaValidatorFactory.validate(xmlDoc);
  }

  /** List the supported schemas. */
  @Operation(
      summary = "List supported EML schemas",
      description = "Returns the supported EML schema locations.")
  @ApiResponses({
    @ApiResponse(
        responseCode = "200",
        description = "Schemas returned",
        content = @Content(schema = @Schema(implementation = String.class)))
  })
  @GetMapping("schemas")
  public List<String> getSchemas() {
    return Arrays.asList(schemaLocations.getEml(), schemaLocations.getEmlGbifProfile());
  }
}
