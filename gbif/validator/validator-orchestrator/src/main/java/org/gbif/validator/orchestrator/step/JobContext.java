package org.gbif.validator.orchestrator.step;

import java.util.List;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;

/**
 * Immutable job identity and configured workflow steps, independent of the WS persistence model.
 */
public record JobContext(UUID datasetUuid, int attempt, List<StepType> pipelineSteps) {

  public JobContext {
    pipelineSteps = List.copyOf(pipelineSteps);
  }
}
