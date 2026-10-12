package org.gbif.validator.orchestrator.step;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;

/** Adapter between one worker message contract and the orchestrator's generic execution model. */
public interface StepDefinition<C> {

  StepType stepType();

  Class<C> completionType();

  Object createRequest(Validation validation);

  CompletionResult interpret(C completion);
}
