package org.gbif.validator.orchestrator.step;

import java.util.Objects;

/** Controls whether a completion-hook failure blocks the workflow. */
public record HookRegistration<C>(CompletionHook<C> hook, boolean required) {

  public HookRegistration {
    Objects.requireNonNull(hook, "hook must not be null");
  }

  public static <C> HookRegistration<C> required(CompletionHook<C> hook) {
    return new HookRegistration<>(hook, true);
  }

  public static <C> HookRegistration<C> optional(CompletionHook<C> hook) {
    return new HookRegistration<>(hook, false);
  }
}
