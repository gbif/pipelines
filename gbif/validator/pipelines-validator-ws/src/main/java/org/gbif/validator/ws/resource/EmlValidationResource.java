package org.gbif.validator.ws.resource;

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

@RestController
@RequestMapping(value = "validation/eml", produces = MediaType.APPLICATION_JSON_VALUE)
@RequiredArgsConstructor
public class EmlValidationResource {

  private final ValidatorWsConfiguration.XmlSchemaLocations schemaLocations;

  private final SchemaValidatorFactory schemaValidatorFactory;

  /** Validates an EML document. */
  @PostMapping(
      consumes = {MediaType.APPLICATION_XML_VALUE},
      produces = {MediaType.APPLICATION_JSON_VALUE})
  @SneakyThrows
  public List<IssueInfo> validateEml(@RequestBody byte[] document) {
    String xmlDoc = new String(document, StandardCharsets.UTF_8);
    return schemaValidatorFactory.validate(xmlDoc);
  }

  /** List the supported schemas. */
  @GetMapping("schemas")
  public List<String> getSchemas() {
    return Arrays.asList(schemaLocations.getEml(), schemaLocations.getEmlGbifProfile());
  }
}
