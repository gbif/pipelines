package org.gbif.validator.orchestrator.service;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Objects;
import org.springframework.stereotype.Service;

/** Narrow transport-independent boundary used by the completion listener. */
@Service
public class DwcdpCompletionHandler {
  private final OrchestrationService orchestration;
  private final DwcdpCompletionInterpreter interpreter = new DwcdpCompletionInterpreter();

  public DwcdpCompletionHandler(OrchestrationService orchestration) {
    this.orchestration = Objects.requireNonNull(orchestration);
  }

  public void handle(JsonNode body) throws IOException {
    var completion = interpreter.interpret(body);
    orchestration.handle(completion.validationKey(), completion.event());
  }
}
