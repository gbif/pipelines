package org.gbif.pipelines.tasks.validators.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import org.gbif.api.model.pipelines.PipelinesWorkflow;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.ValidationWsClientStub;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.api.DwcFileType;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Metrics.FileInfo;
import org.gbif.validator.api.Metrics.IssueInfo;
import org.gbif.validator.api.Metrics.ValidationStep;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.Validation.Status;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Metrics are read from a {@code collect-metrics.json} fixture written to a private, per-test
 * directory, mirroring what the {@code ValidatorMetricsPipeline} Spark job writes to HDFS.
 * Elasticsearch is no longer part of this flow (see {@link
 * org.gbif.pipelines.tasks.validators.metrics.collector.SparkMetricsCollector}), so no ES fixtures
 * are needed here.
 */
@RunWith(MockitoJUnitRunner.class)
public class MetricsCollectorCallbackIT {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();

  private static final PipelinesWorkflow.Graph<StepType> WF =
      PipelinesWorkflow.getValidatorWorkflow();

  @Mock private PipelinesHistoryClient historyClient;

  @After
  public void after() {
    PUBLISHER.close();
  }

  @Test
  public void successInterpretationTest() throws IOException {
    // State
    MetricsCollectorConfiguration config = createConfig();
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, PUBLISHER, historyClient, validationClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    PipelinesIndexedMessage message = createMessage(uuid, attempt);

    writeCollectMetricsFixture(
        uuid,
        attempt,
        "{\"files\":[{\"rowType\":\""
            + DwcTerm.Occurrence.qualifiedName()
            + "\",\"indexedCount\":1,\"issues\":["
            + "{\"issue\":\"GEODETIC_DATUM_ASSUMED_WGS84\",\"count\":1,\"issueCategory\":\"OCC_INTERPRETATION_BASED\"},"
            + "{\"issue\":\"LICENSE_MISSING_OR_UNKNOWN\",\"count\":1,\"issueCategory\":\"OCC_INTERPRETATION_BASED\"}"
            + "]}]}");

    validationClient.update(
        UUID.randomUUID(),
        Validation.builder()
            .key(UUID.randomUUID())
            .metrics(
                Metrics.builder()
                    .stepTypes(
                        Arrays.asList(
                            ValidationStep.builder()
                                .stepType(StepType.VALIDATOR_DWCA_TO_VERBATIM.name())
                                .executionOrder(WF.getLevel(StepType.VALIDATOR_DWCA_TO_VERBATIM))
                                .build(),
                            ValidationStep.builder()
                                .stepType(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED.name())
                                .executionOrder(999)
                                .build(),
                            ValidationStep.builder()
                                .stepType(StepType.VALIDATOR_COLLECT_METRICS.name())
                                .executionOrder(WF.getLevel(StepType.VALIDATOR_COLLECT_METRICS))
                                .build()))
                    .fileInfos(
                        Collections.singletonList(
                            FileInfo.builder()
                                .fileName("verbatim.txt")
                                .fileType(DwcFileType.CORE)
                                .issues(
                                    Collections.singletonList(
                                        IssueInfo.builder().issue("OLD").count(999L).build()))
                                .build()))
                    .build())
            .build());

    // When
    callback.handleMessage(message);

    // Should
    assertEquals(0, PUBLISHER.getMessages().size());

    Validation validation = validationClient.getValidation();

    assertEquals(3, validation.getMetrics().getFileInfos().size());
    assertEquals(Status.QUEUED, validation.getStatus());
    assertFalse(validation.getMetrics().isIndexeable());

    Optional<FileInfo> coreOpt = validationClient.getFileInfo(DwcFileType.CORE, DwcTerm.Occurrence);
    assertTrue(coreOpt.isPresent());

    FileInfo core = coreOpt.get();
    assertEquals("occurrence.txt", core.getFileName());
    assertEquals(Long.valueOf(1534L), core.getCount());
    assertEquals(Long.valueOf(1L), core.getIndexedCount());
    assertEquals(235, core.getTerms().size());
    assertEquals(2, core.getIssues().size());
    assertEquals(DwcFileType.CORE, core.getFileType());

    Optional<IssueInfo> randomIssue =
        core.getIssues().stream()
            .filter(x -> x.getIssue().equals("LICENSE_MISSING_OR_UNKNOWN"))
            .findAny();
    assertTrue(randomIssue.isPresent());
    assertEquals(Long.valueOf(1), randomIssue.get().getCount());

    Optional<IssueInfo> geodeticDatumAssumedWgs84Issue =
        core.getIssues().stream()
            .filter(x -> x.getIssue().equals(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name()))
            .findAny();
    assertTrue(geodeticDatumAssumedWgs84Issue.isPresent());
    assertEquals(Long.valueOf(1), geodeticDatumAssumedWgs84Issue.get().getCount());
    assertEquals(
        EvaluationCategory.OCC_INTERPRETATION_BASED,
        geodeticDatumAssumedWgs84Issue.get().getIssueCategory());

    Optional<FileInfo> extOpt =
        validationClient.getFileInfo(DwcFileType.EXTENSION, Extension.MULTIMEDIA.getRowType());
    assertTrue(extOpt.isPresent());

    FileInfo ext = extOpt.get();

    assertEquals("multimedia.txt", ext.getFileName());
    assertEquals(DwcFileType.EXTENSION, ext.getFileType());
  }

  @Test
  public void successSingleInterpretationTest() throws IOException {
    // State
    MetricsCollectorConfiguration config = createConfig();
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, PUBLISHER, historyClient, validationClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    PipelinesIndexedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_COLLECT_METRICS.name()));

    writeCollectMetricsFixture(
        uuid,
        attempt,
        "{\"files\":[{\"rowType\":\""
            + DwcTerm.Occurrence.qualifiedName()
            + "\",\"indexedCount\":1,\"issues\":["
            + "{\"issue\":\"GEODETIC_DATUM_ASSUMED_WGS84\",\"count\":1,\"issueCategory\":\"OCC_INTERPRETATION_BASED\"},"
            + "{\"issue\":\"RANDOM_ISSUE\",\"count\":1}"
            + "]}]}");

    // When
    callback.handleMessage(message);

    // Should
    assertEquals(0, PUBLISHER.getMessages().size());

    Validation validation = validationClient.getValidation();

    assertEquals(3, validation.getMetrics().getFileInfos().size());
    assertEquals(Status.FINISHED, validation.getStatus());
    assertTrue(validation.getMetrics().isIndexeable());
  }

  @Test
  public void failedCaseTest() {
    // State - no collect-metrics.json fixture is written, so the Spark collector fails to read it
    MetricsCollectorConfiguration config = createConfig();
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, PUBLISHER, historyClient, validationClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    PipelinesIndexedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_COLLECT_METRICS.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertTrue(PUBLISHER.getMessages().isEmpty());
  }

  @Test
  public void successEventInterpretationTest() throws IOException {
    // State
    MetricsCollectorConfiguration config = createConfig();
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, PUBLISHER, historyClient, validationClient);

    String datasetUuid = "9997fa4e-54c1-43ea-9856-afa90204c162";
    UUID uuid = UUID.fromString(datasetUuid);
    int attempt = 60;

    PipelinesIndexedMessage message = createMessage(uuid, attempt);

    // Event-core datasets don't have an Occurrence-rowType file to overlay Spark metrics onto,
    // so the fixture only needs to exist for the collector to succeed reading it.
    writeCollectMetricsFixture(uuid, attempt, "{\"files\":[]}");

    validationClient.update(
        UUID.randomUUID(),
        Validation.builder()
            .key(UUID.randomUUID())
            .metrics(
                Metrics.builder()
                    .stepTypes(
                        Arrays.asList(
                            ValidationStep.builder()
                                .stepType(StepType.VALIDATOR_DWCA_TO_VERBATIM.name())
                                .executionOrder(WF.getLevel(StepType.VALIDATOR_DWCA_TO_VERBATIM))
                                .build(),
                            ValidationStep.builder()
                                .stepType(StepType.VALIDATOR_VERBATIM_TO_INTERPRETED.name())
                                .executionOrder(999)
                                .build(),
                            ValidationStep.builder()
                                .stepType(StepType.VALIDATOR_COLLECT_METRICS.name())
                                .executionOrder(WF.getLevel(StepType.VALIDATOR_COLLECT_METRICS))
                                .build()))
                    .fileInfos(
                        Collections.singletonList(
                            FileInfo.builder()
                                .fileName("example.txt")
                                .fileType(DwcFileType.EXTENSION)
                                .issues(
                                    Collections.singletonList(
                                        IssueInfo.builder().issue("OLD").count(999L).build()))
                                .build()))
                    .build())
            .build());

    // When
    callback.handleMessage(message);

    // Should
    assertEquals(0, PUBLISHER.getMessages().size());

    Validation validation = validationClient.getValidation();

    assertEquals(4, validation.getMetrics().getFileInfos().size());
    assertEquals(Status.QUEUED, validation.getStatus());
    assertFalse(validation.getMetrics().isIndexeable());

    Optional<FileInfo> coreOpt = validationClient.getFileInfo(DwcFileType.CORE, DwcTerm.Event);
    assertTrue(coreOpt.isPresent());

    FileInfo core = coreOpt.get();
    assertEquals("event.txt", core.getFileName());
    assertEquals(Long.valueOf(30L), core.getCount());
  }

  private void writeCollectMetricsFixture(UUID datasetKey, int attempt, String json)
      throws IOException {
    Path attemptDir =
        folder.getRoot().toPath().resolve(datasetKey.toString()).resolve(String.valueOf(attempt));
    Files.createDirectories(attemptDir);
    Files.write(attemptDir.resolve("collect-metrics.json"), json.getBytes(StandardCharsets.UTF_8));
  }

  private PipelinesIndexedMessage createMessage(UUID uuid, int attempt) {
    PipelinesIndexedMessage message = new PipelinesIndexedMessage();

    message.setDatasetUuid(uuid);
    message.setAttempt(attempt);
    message.setExecutionId(EXECUTION_ID);
    message.setRunner(StepRunner.STANDALONE.name());
    message.setEndpointType(EndpointType.DWC_ARCHIVE);
    message.setPipelineSteps(
        new HashSet<>(
            Arrays.asList(
                StepType.VALIDATOR_INTERPRETED_TO_INDEX.name(),
                StepType.VALIDATOR_COLLECT_METRICS.name())));
    return message;
  }

  private MetricsCollectorConfiguration createConfig() {
    MetricsCollectorConfiguration config = new MetricsCollectorConfiguration();
    // Main
    config.archiveRepository =
        this.getClass().getClassLoader().getResource("dataset/dwca").getPath();
    config.validatorOnly = true;

    // Step config - points at a private per-test directory holding collect-metrics.json fixtures
    config.stepConfig.repositoryPath = folder.getRoot().getPath();
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    return config;
  }
}
