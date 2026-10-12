package org.gbif.validator.orchestrator.step;

import org.gbif.api.model.pipelines.StepType;

@FunctionalInterface
public interface HookFailureReporter {

  void report(StepType stepType, Exception exception);
}
