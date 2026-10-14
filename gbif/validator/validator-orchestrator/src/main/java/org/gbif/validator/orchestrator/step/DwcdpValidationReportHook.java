package org.gbif.validator.orchestrator.step;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes analyser output before committing its completion transition. */
public final class DwcdpValidationReportHook implements CompletionHook<JsonNode> {

  private static final Logger LOG = LoggerFactory.getLogger(DwcdpValidationReportHook.class);

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
    LOG.info(
        "Uploaded validation report validation={} attempt={} type={}",
        validation.getKey(),
        completion.path("attempt").asInt(),
        ValidationReportTypes.DWCDP_ANALYSIS);
  }
}
