package org.gbif.validator.orchestrator.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import org.gbif.validator.api.Validation;
import org.gbif.validator.orchestrator.support.RecordingValidationClient;
import org.junit.jupiter.api.Test;

class DwcdpValidationReportHookTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void writesRawJsonReport() {
    RecordingValidationClient client = new RecordingValidationClient();
    UUID key = UUID.randomUUID();
    Validation validation = Validation.builder().key(key).build();

    var completion = mapper.createObjectNode();
    completion.put("attempt", 1);
    completion.set("validationReport", mapper.createObjectNode().put("count", 12));

    new DwcdpValidationReportHook(client).execute(validation, completion);

    assertEquals(1, client.getReportWrites());
    assertEquals(key, client.getReportKey());
    assertEquals(1, client.getReportAttempt());
    assertEquals(ValidationReportTypes.DWCDP_ANALYSIS, client.getReportType());
    assertEquals("{\"count\":12}", client.getReportJson());
  }

  @Test
  void rejectsMissingReport() {
    RecordingValidationClient client = new RecordingValidationClient();
    Validation validation = Validation.builder().key(UUID.randomUUID()).build();
    var completion = mapper.createObjectNode().put("attempt", 1);

    assertThrows(
        IllegalArgumentException.class,
        () -> new DwcdpValidationReportHook(client).execute(validation, completion));

    assertEquals(0, client.getReportWrites());
  }

}
