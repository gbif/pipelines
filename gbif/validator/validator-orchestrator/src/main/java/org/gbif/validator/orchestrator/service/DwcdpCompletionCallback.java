package org.gbif.validator.orchestrator.service;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Objects;
import org.gbif.common.messaging.api.MessageCallback;
import org.springframework.stereotype.Component;

/** Adapts postal-api's unchecked callback contract to the completion handler. */
@Component
public class DwcdpCompletionCallback implements MessageCallback<JsonNode> {

  private final DwcdpCompletionHandler handler;

  public DwcdpCompletionCallback(DwcdpCompletionHandler handler) {
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
  }

  @Override
  public Class<JsonNode> getMessageClass() {
    return JsonNode.class;
  }

  @Override
  public void handleMessage(JsonNode message) {
    try {
      handler.handle(message);
    } catch (IOException e) {
      throw new OrchestrationProcessingException("Failed to process DWc-DP completion", e);
    }
  }
}
