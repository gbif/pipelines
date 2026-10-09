package org.gbif.validator.orchestrator.service;

import java.util.Map;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;
import org.springframework.stereotype.Component;

/** Uses the JSON contract currently accepted by dwcdp-analyser-service. */
@Component
public class DwcdpAnalysisRequestFactory implements StepRequestFactory {
  @Override
  public boolean supports(StepType step) {
    return step == StepType.VALIDATOR_VALIDATE_DWCDP;
  }

  @Override
  public Object create(StepType step, Validation validation) {
    if (!supports(step)) {
      throw new IllegalArgumentException("Unsupported step: " + step);
    }

    // Current validator implementation uses attempt=1.
    return Map.of("datasetUuid", validation.getKey().toString(), "attempt", 1);
  }
}
