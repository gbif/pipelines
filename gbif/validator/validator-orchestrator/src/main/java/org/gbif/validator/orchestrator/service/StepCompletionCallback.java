package org.gbif.validator.orchestrator.service;

import java.util.Objects;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.MessageCallback;

/** Adapts a typed completion to the generic completion processor. */
public final class StepCompletionCallback<T> implements MessageCallback<T> {

  @FunctionalInterface
  public interface CompletionHandler {
    void complete(StepType stepType, Object payload) throws Exception;
  }

  private final StepType stepType;
  private final Class<T> messageType;
  private final CompletionHandler handler;

  public StepCompletionCallback(
      StepType stepType, Class<T> messageType, CompletionHandler handler) {
    this.stepType = Objects.requireNonNull(stepType, "stepType must not be null");
    this.messageType = Objects.requireNonNull(messageType, "messageType must not be null");
    this.handler = Objects.requireNonNull(handler, "handler must not be null");
  }

  @Override
  public Class<T> getMessageClass() {
    return messageType;
  }

  @Override
  public void handleMessage(T message) {
    try {
      handler.complete(stepType, message);
    } catch (Exception exception) {
      throw new OrchestrationProcessingException(
          "Failed to process completion for " + stepType, exception);
    }
  }
}
