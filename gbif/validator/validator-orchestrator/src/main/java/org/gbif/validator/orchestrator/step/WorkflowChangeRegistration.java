package org.gbif.validator.orchestrator.step;

import java.util.Objects;

public record WorkflowChangeRegistration<C>(WorkflowChangeHook<C> hook, boolean required) {

  public WorkflowChangeRegistration {
    Objects.requireNonNull(hook, "hook");
  }

  public static <C> WorkflowChangeRegistration<C> required(WorkflowChangeHook<C> hook) {
    return new WorkflowChangeRegistration<>(hook, true);
  }

  public static <C> WorkflowChangeRegistration<C> optional(WorkflowChangeHook<C> hook) {
    return new WorkflowChangeRegistration<>(hook, false);
  }
}
