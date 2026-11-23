package org.gbif.pipelines.coordinator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DwcDpToVerbatimMessage;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.api.report.ReportTypes;
import org.gbif.orchestrator.api.step.Outcome;
import org.gbif.orchestrator.api.step.OutputKeys;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.pipelines.common.PipelinesVariables.Metrics;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.core.utils.MetricsUtil;
import org.gbif.pipelines.orchestrator.OrchestratorReplies;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A validator step dispatched by the flow-orchestrator replies with a StepCompleted. */
class OrchestratedValidatorStepTest {

  private static final UUID WORKFLOW = UUID.fromString("0b6c1b5e-6a4c-4d7e-9a43-6a0f4c1f7d10");
  private static final int ATTEMPT = 2;

  @Test
  void conversionRepliesWithOutputsAndReportAndUsesStagedShape(@TempDir Path tempDir)
      throws Exception {
    UUID dataset = UUID.randomUUID();
    RecordingPublisher publisher = new RecordingPublisher();
    List<DwcDpToVerbatimMessage> ran = new ArrayList<>();

    try (FileSystem fs = FileSystem.getLocal(new Configuration())) {
      String dir = "file://" + tempDir + "/" + dataset + "/" + ATTEMPT;
      fs.mkdirs(new org.apache.hadoop.fs.Path(dir + "/dwcdp-to-verbatim-report"));
      MetricsUtil.writeMetricsYaml(
          fs,
          Map.of("COUNT_OCCURRENCE", 12L, "COUNT_EVENT", 0L, "COUNT_MAX", 12L),
          dir + "/dwcdp-stage.yml");
      MetricsUtil.writeMetricsYaml(
          fs,
          Map.of(Metrics.ARCHIVE_TO_OCC_COUNT, 12L, Metrics.EVENT_CORE_RECORDS_COUNT, 0L),
          dir + "/archive-to-verbatim.yml");
      write(fs, dir + "/dwcdp-to-verbatim-report/compact.json", "{\"targets\":[\"occurrence\"]}");

      ValidatorDwcDpToVerbatimCallback callback =
          new ValidatorDwcDpToVerbatimCallback(config(tempDir), publisher, null) {
            @Override
            protected void runPipeline(DwcDpToVerbatimMessage message) {
              ran.add(message);
            }
          };
      callback.fileSystem = fs;

      // flags deliberately wrong: the staged counts decide
      DwcDpToVerbatimMessage message = message(dataset, true, false);
      callback.handleMessage(message);

      assertEquals(1, ran.size());
      assertFalse(ran.get(0).isContainsEvents());
      assertTrue(ran.get(0).isContainsOccurrences());

      StepCompleted completed = publisher.onlyCompletion();
      assertEquals(Outcome.SUCCEEDED, completed.outcome());
      assertEquals(header(dataset), completed.header());
      assertEquals(
          Map.of(
              OutputKeys.DATASET_TYPE, "OCCURRENCE",
              OutputKeys.OCCURRENCE_RECORDS, "12",
              OutputKeys.EVENT_RECORDS, "0"),
          completed.outputs());
      assertEquals(1, completed.reports().size());
      assertEquals(ReportTypes.DWCDP_CONVERSION, completed.reports().get(0).type());
      assertEquals(
          "occurrence",
          completed.reports().get(0).content().path("compact").path("targets").get(0).asText());
      assertEquals("orchestrator", publisher.exchange);
      assertEquals("validation.step.completed", publisher.routingKey);
    }
  }

  @Test
  void failureRepliesFailedWithDiagnostic(@TempDir Path tempDir) throws Exception {
    UUID dataset = UUID.randomUUID();
    RecordingPublisher publisher = new RecordingPublisher();

    try (FileSystem fs = FileSystem.getLocal(new Configuration())) {
      // no dwcdp-stage.yml: the staged shape cannot be read
      ValidatorDwcDpToVerbatimCallback callback =
          new ValidatorDwcDpToVerbatimCallback(config(tempDir), publisher, null);
      callback.fileSystem = fs;

      callback.handleMessage(message(dataset, false, true));

      StepCompleted completed = publisher.onlyCompletion();
      assertEquals(Outcome.FAILED, completed.outcome());
      assertTrue(completed.diagnostic().contains("dwcdp-stage.yml"), completed.diagnostic());
      assertTrue(completed.reports().isEmpty());
    }
  }

  @Test
  void headerIsReadFromTheMessage() {
    UUID dataset = UUID.randomUUID();
    DwcDpToVerbatimMessage plain = message(dataset, true, true);
    plain.setStepHeader(null);
    assertTrue(OrchestratorReplies.header(plain).isEmpty());

    assertEquals(header(dataset), OrchestratorReplies.header(message(dataset, true, true)).get());

    DwcDpToVerbatimMessage broken = message(dataset, true, true);
    broken.setStepHeader(Map.of("workflowId", WORKFLOW.toString()));
    assertThrows(IllegalArgumentException.class, () -> OrchestratorReplies.header(broken));
  }

  @Test
  void completionsUseIsoTimestamps() throws Exception {
    RecordingPublisher publisher = new RecordingPublisher();
    OrchestratorReplies replies =
        new OrchestratorReplies(publisher, config(Path.of("/tmp")).getOrchestrator());
    StepCompleted completed =
        new StepCompleted(
            UUID.randomUUID(),
            header(UUID.randomUUID()),
            Outcome.SUCCEEDED,
            null,
            null,
            null,
            java.time.Instant.parse("2026-11-18T10:00:00Z"),
            java.time.Instant.parse("2026-11-18T10:01:00Z"));

    replies.send(completed);

    JsonNode body = (JsonNode) publisher.sent.get(0);
    assertEquals("2026-11-18T10:00:00Z", body.get("startedAt").asText());
    assertNull(body.get("diagnostic"));
  }

  private static DwcDpToVerbatimMessage message(UUID dataset, boolean events, boolean occurrences) {
    DwcDpToVerbatimMessage message =
        new DwcDpToVerbatimMessage(
            dataset,
            ATTEMPT,
            Set.of(StepType.VALIDATOR_DWCDP_TO_VERBATIM.name()),
            null,
            occurrences,
            events,
            true);
    message.setStepHeader(OrchestratorJson.mapper().convertValue(header(dataset), Map.class));
    return message;
  }

  private static StepHeader header(UUID dataset) {
    return new StepHeader(WORKFLOW, dataset, ATTEMPT, StepType.VALIDATOR_DWCDP_TO_VERBATIM, 1);
  }

  private static PipelinesConfig config(Path tempDir) {
    PipelinesConfig config = new PipelinesConfig();
    config.setOutputPath("file://" + tempDir);
    config.setInputPath("file://" + tempDir);
    config.getStandalone().getRegistry().setWsUrl("http://localhost:0");
    config.getStandalone().getRegistry().setUser("test");
    config.getStandalone().getRegistry().setPassword("test");
    config.getOrchestrator().setCompletionRoutingKey("validation.step.completed");
    return config;
  }

  private static void write(FileSystem fs, String path, String content) throws Exception {
    try (OutputStream out = fs.create(new org.apache.hadoop.fs.Path(path))) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }

  /** Records what is sent with an explicit exchange and routing key; anything else fails. */
  private static class RecordingPublisher implements MessagePublisher {
    final List<Object> sent = new ArrayList<>();
    String exchange;
    String routingKey;

    StepCompleted onlyCompletion() {
      assertEquals(1, sent.size(), "exactly one reply");
      return OrchestratorJson.mapper().convertValue(sent.get(0), StepCompleted.class);
    }

    @Override
    public void send(Object message, String exchange, String routingKey) {
      sent.add(message);
      this.exchange = exchange;
      this.routingKey = routingKey;
    }

    @Override
    public void send(Object message, String exchange, String routingKey, boolean persistent) {
      send(message, exchange, routingKey);
    }

    @Override
    public void send(Message message) {
      throw new AssertionError("no next-step message expected: " + message);
    }

    @Override
    public void send(Message message, boolean persistent) {
      send(message);
    }

    @Override
    public void send(Message message, String exchange) {
      send(message);
    }

    @Override
    public void replyToQueue(
        Object message, boolean persistent, String correlationId, String replyTo) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T sendAndReceive(
        Message message, String routingKey, boolean persistent, String correlationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T sendAndReceive(
        Object message,
        String exchange,
        String routingKey,
        boolean persistent,
        String correlationId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}
  }
}
