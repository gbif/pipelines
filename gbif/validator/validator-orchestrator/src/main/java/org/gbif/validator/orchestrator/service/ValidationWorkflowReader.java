package org.gbif.validator.orchestrator.service;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.validator.api.Validation;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.springframework.stereotype.Service;

/** Read-only workflow access; workflow mutations are intentionally deferred. */
@Service
public class ValidationWorkflowReader {
  private final ValidationWsClient client;
  private final StepSelector selector;

  public ValidationWorkflowReader(ValidationWsClient client, StepSelector selector) {
    this.client = Objects.requireNonNull(client);
    this.selector = Objects.requireNonNull(selector);
  }

  public Validation get(UUID validationUuid) {
    return client.get(validationUuid);
  }

  public Optional<StepType> nextPendingStep(UUID validationUuid) {
    return selector.nextPendingStep(get(validationUuid));
  }
}
