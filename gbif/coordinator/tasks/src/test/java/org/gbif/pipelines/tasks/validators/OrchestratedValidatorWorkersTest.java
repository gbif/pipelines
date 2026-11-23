package org.gbif.pipelines.tasks.validators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.common.messaging.api.Message;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.PipelinesArchiveValidatorMessage;
import org.gbif.common.messaging.api.messages.PipelinesChecklistValidatorMessage;
import org.gbif.dwca.validation.xml.SchemaValidatorFactory;
import org.gbif.orchestrator.api.json.OrchestratorJson;
import org.gbif.orchestrator.api.step.StepCompleted;
import org.gbif.orchestrator.api.step.StepHeader;
import org.gbif.pipelines.tasks.validators.checklist.ChecklistValidatorCallback;
import org.gbif.pipelines.tasks.validators.checklist.ChecklistValidatorConfiguration;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorCallback;
import org.gbif.pipelines.tasks.validators.validator.ArchiveValidatorConfiguration;
import org.gbif.pipelines.validator.checklist.ChecklistValidator;
import org.gbif.pipelines.validator.checklist.ChecklistbankWsClient;
import org.gbif.validator.api.ClbDatasetImport;
import org.gbif.validator.api.Metrics.FileInfo;
import org.junit.Test;

/**
 * The CLI coordinator's validator steps in orchestrator mode: a message with a step header gets a
 * StepCompleted on the orchestrator's completion routing key, and nothing else (no validator-ws, no
 * next message).
 */
public class OrchestratedValidatorWorkersTest {

  private static final UUID WORKFLOW = UUID.fromString("0b6c1b5e-6a4c-4d7e-9a43-6a0f4c1f7d10");
  private static final UUID DWCA = UUID.fromString("9bed66b3-4caa-42bb-9c93-71d7ba109dad");
  private static final String COMPLETED = "validation.step.completed";
  private static final ObjectMapper JSON = OrchestratorJson.mapper();

  @Test
  public void archiveValidatorRepliesWithTheReportAndTheShape() throws Exception {
    RecordingPublisher publisher = new RecordingPublisher();

    archiveCallback(publisher).handleMessage(archiveMessage(DWCA, "DWCA"));

    StepCompleted completed = publisher.onlyCompletion();
    assertEquals("SUCCEEDED", completed.outcome().name());
    assertEquals(header(DWCA, StepType.VALIDATOR_VALIDATE_ARCHIVE), completed.header());
    assertEquals("OCCURRENCE", completed.outputs().get("datasetType"));
    JsonNode report = completed.reports().get(0).content();
    assertEquals("ARCHIVE_VALIDATION", completed.reports().get(0).type());
    assertTrue(report.get("valid").asBoolean());
    // The orchestrator's JSON leaves null fields out.
    assertFalse(report.hasNonNull("invalidationReason"));
    List<String> files = new ArrayList<>();
    report.get("files").forEach(f -> files.add(f.path("fileName").asText()));
    assertEquals(List.of("eml.xml", "occurrence.txt"), files);
    assertTrue("no legacy messages", publisher.messages.isEmpty());
  }

  @Test
  public void anUnsupportedFormatFailsTheStep() throws Exception {
    RecordingPublisher publisher = new RecordingPublisher();

    archiveCallback(publisher).handleMessage(archiveMessage(DWCA, "XML"));

    StepCompleted completed = publisher.onlyCompletion();
    assertEquals("FAILED", completed.outcome().name());
    assertTrue(completed.diagnostic(), completed.diagnostic().contains("no orchestrated"));
  }

  @Test
  public void checklistValidatorReportsAFinishedImport() throws Exception {
    RecordingPublisher publisher = new RecordingPublisher();

    checklistCallback(publisher)
        .handleMessage(
            checklistMessage("{\"datasetKey\":4711,\"status\":\"finished\",\"format\":\"coldp\"}"));

    StepCompleted completed = publisher.onlyCompletion();
    assertEquals("SUCCEEDED", completed.outcome().name());
    assertEquals(Map.of("datasetType", "CHECKLIST"), completed.outputs());
    JsonNode report = completed.reports().get(0).content();
    assertEquals("CHECKLIST", completed.reports().get(0).type());
    assertEquals(4711, report.get("clbDatasetKey").asInt());
    assertTrue("ColDP is indexeable", report.get("indexeable").asBoolean());
    assertEquals("NameUsage.tsv", report.get("files").get(0).get("fileName").asText());
  }

  @Test
  public void checklistValidatorFailsAnImportThatDidNotFinish() throws Exception {
    RecordingPublisher publisher = new RecordingPublisher();

    checklistCallback(publisher)
        .handleMessage(checklistMessage("{\"datasetKey\":4711,\"status\":\"running\"}"));
    checklistCallback(publisher).handleMessage(checklistMessage(null));

    assertEquals(2, publisher.completions.size());
    for (StepCompleted completed : publisher.completions) {
      assertEquals("FAILED", completed.outcome().name());
      assertFalse(completed.diagnostic().isBlank());
    }
    assertTrue(publisher.completions.get(0).diagnostic().contains("running"));
  }

  // --- helpers ---------------------------------------------------------------------------------

  private ArchiveValidatorCallback archiveCallback(MessagePublisher publisher) {
    ArchiveValidatorConfiguration config = new ArchiveValidatorConfiguration();
    config.archiveRepository = getClass().getResource("/dataset/dwca").getPath();
    config.validatorOnly = true;
    config.stepConfig.orchestrator.completionRoutingKey = COMPLETED;
    return new ArchiveValidatorCallback(
        config, publisher, null, null, new SchemaValidatorFactory());
  }

  private ChecklistValidatorCallback checklistCallback(MessagePublisher publisher) {
    ChecklistValidatorConfiguration config = new ChecklistValidatorConfiguration();
    config.orchestrator.completionRoutingKey = COMPLETED;
    ChecklistValidator checklistbank =
        new ChecklistValidator((ChecklistbankWsClient) null, "https://api.gbif-dev.org/v1/", null) {
          @Override
          public List<FileInfo> evaluateResults(ClbDatasetImport clbDatasetImport) {
            return List.of(FileInfo.builder().fileName("NameUsage.tsv").count(3L).build());
          }
        };
    return new ChecklistValidatorCallback(config, null, publisher, checklistbank);
  }

  private static PipelinesArchiveValidatorMessage archiveMessage(UUID key, String format) {
    PipelinesArchiveValidatorMessage message =
        new PipelinesArchiveValidatorMessage(
            key, 1, java.util.Set.of(StepType.VALIDATOR_VALIDATE_ARCHIVE.name()), null, format);
    message.setStepHeader(map(header(key, StepType.VALIDATOR_VALIDATE_ARCHIVE)));
    return message;
  }

  private static PipelinesChecklistValidatorMessage checklistMessage(String payload) {
    UUID key = UUID.fromString("673f2308-3501-474f-85d5-a832825bb46c");
    return new PipelinesChecklistValidatorMessage(key, payload)
        .setStepHeader(map(header(key, StepType.VALIDATOR_CHECKLISTBANK)));
  }

  private static StepHeader header(UUID key, StepType type) {
    return new StepHeader(WORKFLOW, key, 1, type, 1);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(StepHeader header) {
    return JSON.convertValue(header, Map.class);
  }

  /** Records completions sent to the orchestrator, and any other message (there should be none). */
  static class RecordingPublisher implements MessagePublisher {
    final List<StepCompleted> completions = new ArrayList<>();
    final List<Object> messages = new ArrayList<>();

    StepCompleted onlyCompletion() {
      assertEquals(1, completions.size());
      return completions.get(0);
    }

    @Override
    public void send(Message message) {
      messages.add(message);
    }

    @Override
    public void send(Message message, boolean persistent) {
      messages.add(message);
    }

    @Override
    public void send(Message message, String exchange) {
      messages.add(message);
    }

    @Override
    public void send(Object message, String exchange, String routingKey) {
      assertEquals("orchestrator", exchange);
      assertEquals(COMPLETED, routingKey);
      completions.add(JSON.convertValue(message, StepCompleted.class));
    }

    @Override
    public void send(Object message, String exchange, String routingKey, boolean persistent) {
      send(message, exchange, routingKey);
    }

    @Override
    public void replyToQueue(
        Object message, boolean persistent, String correlationId, String replyTo) {}

    @Override
    public <T> T sendAndReceive(Message message, String routingKey, boolean persistent, String r) {
      return null;
    }

    @Override
    public <T> T sendAndReceive(
        Object message, String exchange, String routingKey, boolean persistent, String r) {
      return null;
    }

    @Override
    public void close() {}
  }
}
