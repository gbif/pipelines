package org.gbif.validator.orchestrator.step;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;

/** Immutable collection of registered step adapters. Duplicate registration is an error. */
public final class StepRegistry {

  private final Map<StepType, StepRegistration<?>> registrations;

  public StepRegistry(List<StepRegistration<?>> registrations) {
    Objects.requireNonNull(registrations, "registrations must not be null");
    Map<StepType, StepRegistration<?>> collected = new EnumMap<>(StepType.class);

    for (StepRegistration<?> registration : registrations) {
      StepType step = registration.stepType();

      if (collected.putIfAbsent(step, registration) != null) {
        throw new IllegalArgumentException("Duplicate step registration: " + step);
      }
    }

    this.registrations = Map.copyOf(collected);
  }

  public StepRegistration<?> require(StepType stepType) {
    StepRegistration<?> registration = registrations.get(stepType);

    if (registration == null) {
      throw new IllegalArgumentException("No adapter registered for " + stepType);
    }

    return registration;
  }
}
