package org.gbif.validator.orchestrator.service;

import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;

/** Converts an execution into the worker-specific request payload. */
public interface StepRequestFactory {
  boolean supports(StepType step);
  Object create(StepType step, Validation validation);
}
