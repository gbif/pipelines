package org.gbif.validator.ws.resource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.ResponseStatusException;

class EmlValidationResourceTest {

  @Test
  void documentsAboveTheMaxSizeAreRejected() {
    EmlValidationResource resource = new EmlValidationResource(null, new SchemaValidatorFactory());
    ReflectionTestUtils.setField(resource, "maxSize", "10B");

    ResponseStatusException ex =
        assertThrows(
            ResponseStatusException.class,
            () -> resource.validateEml("<eml>too large</eml>".getBytes(StandardCharsets.UTF_8)));
    assertEquals(HttpStatus.PAYLOAD_TOO_LARGE, ex.getStatusCode());
  }

  @Test
  void documentsWithinTheMaxSizeAreValidated() {
    EmlValidationResource resource = new EmlValidationResource(null, new SchemaValidatorFactory());
    ReflectionTestUtils.setField(resource, "maxSize", "1KB");

    // no schemaLocation: validated and reported as an issue, not rejected
    assertEquals(1, resource.validateEml("<eml/>".getBytes(StandardCharsets.UTF_8)).size());
  }
}
