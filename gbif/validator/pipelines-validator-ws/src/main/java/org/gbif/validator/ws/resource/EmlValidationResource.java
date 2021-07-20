package org.gbif.validator.ws.resource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.validator.api.XmlSchemaValidatorResult;
import org.gbif.validator.api.XmlSchemaValidatorResult.XmlSchemaValidatorError;
import org.gbif.validator.ws.config.ValidatorWsConfiguration;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.xml.sax.SAXParseException;

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
  public XmlSchemaValidatorResult validateEml(@RequestBody byte[] document) {
    try {
      String xmlDoc = new String(document, StandardCharsets.UTF_8);
      return schemaValidatorFactory.newValidatorFromDocument(xmlDoc).validate(xmlDoc);
    } catch (Throwable ex) {
      return toValidationError(ex);
    }
  }

  /**
   * Translates a Throwable into a XmlSchemaValidator.ValidationResult if possible. Otherwise, it
   * propagates the error,
   */
  private XmlSchemaValidatorResult toValidationError(Throwable ex) {
    if (ex instanceof SAXParseException) {
      return XmlSchemaValidatorResult.builder()
          .errors(
              Collections.singletonList(
                  XmlSchemaValidatorError.builder().error(ex.getMessage()).build()))
          .build();
    }
    throw new RuntimeException(ex);
  }

  /** List the supported schemas. */
  @GetMapping("schemas")
  public List<String> getSchemas() {
    return Arrays.asList(schemaLocations.getEml(), schemaLocations.getEmlGbifProfile());
  }
}
