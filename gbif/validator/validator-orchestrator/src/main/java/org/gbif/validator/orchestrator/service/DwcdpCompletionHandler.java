package org.gbif.validator.orchestrator.service;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.orchestrator.step.CompletionProcessor;
import org.springframework.stereotype.Service;

/** Thin adapter for the existing postal-api DWc-DP completion listener. */
@Service
public class DwcdpCompletionHandler {

  private final CompletionProcessor processor;

  public DwcdpCompletionHandler(CompletionProcessor processor) {
    this.processor = Objects.requireNonNull(processor);
  }

  public void handle(JsonNode body) throws Exception {
    processor.complete(StepType.VALIDATOR_VALIDATE_DWCDP, body);
  }
}
