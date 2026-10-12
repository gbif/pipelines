package org.gbif.validator.orchestrator.step;

import java.util.Objects;
import java.util.UUID;

/** Transport-neutral identification and processing result of a worker completion. */
public record CompletionResult(UUID validationKey, int attempt, boolean successful, String details) {

  public CompletionResult {
    Objects.requireNonNull(validationKey, "validationKey must not be null");

    if (attempt < 1) {
      throw new IllegalArgumentException("attempt must be positive");
    }
  }
}
