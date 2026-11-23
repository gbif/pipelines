package org.gbif.pipelines.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpStageMessage;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.common.messaging.api.messages.PipelineBasedMessage;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesDwcaMessage;
import org.gbif.common.messaging.api.messages.PipelinesInterpretedMessage;
import org.gbif.common.messaging.api.messages.PipelinesVerbatimMessage;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.Report;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.pipelines.core.config.model.OrchestratorConfig;

/**
 * Replies of steps that the flow-orchestrator dispatched. Such a message carries the orchestrator's
 * step header; the step then reports its result as a {@link StepCompleted} to the orchestrator
 * instead of updating validator-ws and sending the next step's message.
 *
 * <p>Shared by the CLI coordinator (archive validator, DwC-A to verbatim, checklist validator) and
 * the spark-coordinator (DwC-DP and the shared identifier, interpretation and metrics steps).
 */
@Slf4j
public final class OrchestratorReplies {

  private static final ObjectMapper JSON = OrchestratorJson.mapper();

  private final MessagePublisher publisher;
  private final OrchestratorConfig config;

  public OrchestratorReplies(MessagePublisher publisher, OrchestratorConfig config) {
    this.publisher = publisher;
    this.config = Objects.requireNonNull(config, "orchestrator config");
  }

  /**
   * The step header of a message dispatched by the orchestrator, or empty for any other message.
   *
   * @throws IllegalArgumentException if the header is present but not a valid {@link StepHeader}
   */
  public static Optional<StepHeader> header(PipelineBasedMessage message) {
    return header(rawHeader(message));
  }

  /**
   * A step header as postal messages carry it (an untyped map), or empty for none.
   *
   * @throws IllegalArgumentException if the header is present but not a valid {@link StepHeader}
   */
  public static Optional<StepHeader> header(Map<String, Object> raw) {
    if (raw == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(JSON.convertValue(raw, StepHeader.class));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid orchestrator step header: " + raw, e);
    }
  }

  private static Map<String, Object> rawHeader(PipelineBasedMessage message) {
    if (message instanceof DwcDpStageMessage m) {
      return m.getStepHeader();
    }
    if (message instanceof DwcDpToVerbatimMessage m) {
      return m.getStepHeader();
    }
    if (message instanceof PipelinesVerbatimMessage m) {
      return m.getStepHeader();
    }
    if (message instanceof PipelinesInterpretedMessage m) {
      return m.getStepHeader();
    }
    if (message instanceof PipelinesArchiveValidatorMessage m) {
      return m.getStepHeader();
    }
    if (message instanceof PipelinesDwcaMessage m) {
      return m.getStepHeader();
    }
    return null;
  }

  /** A successful step, with what it reported. */
  public static StepCompleted succeeded(
      StepHeader header, Instant started, Map<String, String> outputs, List<Report> reports) {
    return new StepCompleted(
        UUID.randomUUID(),
        header,
        Outcome.SUCCEEDED,
        null,
        outputs,
        reports,
        started,
        Instant.now());
  }

  /** A failed step: the diagnostic is the exception's class and message. */
  public static StepCompleted failed(StepHeader header, Instant started, Throwable error) {
    return new StepCompleted(
        UUID.randomUUID(),
        header,
        Outcome.FAILED,
        diagnostic(error),
        Map.of(),
        List.of(),
        started,
        Instant.now());
  }

  /** {@code ExceptionClass: message}, for a failed step's diagnostic. */
  public static String diagnostic(Throwable error) {
    Throwable cause =
        error.getCause() != null && error.getMessage() == null ? error.getCause() : error;
    String message = cause.getMessage();
    return cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
  }

  /** A report whose content is any JSON-serializable value. */
  public static Report report(String type, Object content) {
    return new Report(type, JSON.valueToTree(content), null);
  }

  /**
   * Sends the completion, serialized with the orchestrator's JSON settings.
   *
   * @throws IllegalStateException if no completion routing key is configured
   */
  public void send(StepCompleted completed) throws IOException {
    if (config.getCompletionRoutingKey() == null || config.getCompletionRoutingKey().isBlank()) {
      throw new IllegalStateException(
          "orchestrator.completionRoutingKey is not configured; cannot reply to "
              + completed.header());
    }
    JsonNode body = JSON.valueToTree(completed);
    publisher.send(body, config.getExchange(), config.getCompletionRoutingKey());
    log.info(
        "Sent {} completion for workflow {} step {} attempt {} to {} / {}",
        completed.outcome(),
        completed.header().workflowId(),
        completed.header().stepType(),
        completed.header().stepAttempt(),
        config.getExchange(),
        config.getCompletionRoutingKey());
  }
}
