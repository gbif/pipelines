package org.gbif.validator.orchestrator.step;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;

/** Writes analyser output before committing its completion transition. */
public final class DwcdpValidationReportHook implements CompletionHook<JsonNode> {

  private final ValidationWsClient client;

  public DwcdpValidationReportHook(ValidationWsClient client) {
    this.client = Objects.requireNonNull(client, "client");
  }

  @Override
  public void execute(Validation validation, JsonNode completion) {
    JsonNode report = completion.path("validationReport");

    if (report.isMissingNode() || report.isNull()) {
      throw new IllegalArgumentException("DWc-DP completion has no validationReport");
    }

    client.createOrUpdateValidationReport(
        validation.getKey(),
        completion.path("attempt").asInt(),
        ValidationReportTypes.DWCDP_ANALYSIS,
        report.toString());
  }
}
