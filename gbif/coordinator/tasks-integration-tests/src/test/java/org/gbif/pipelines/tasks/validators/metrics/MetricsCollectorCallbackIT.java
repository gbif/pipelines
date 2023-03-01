package org.gbif.pipelines.tasks.validators.metrics;

import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.ValidationWsClientStub;
import org.gbif.pipelines.tasks.resources.EsServer;
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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MetricsCollectorCallbackIT {

  @ClassRule public static final EsServer ES_SERVER = EsServer.getInstance();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static final Path MAPPINGS_PATH = Paths.get("mappings/verbatim-mapping.json");
  private static final MessagePublisherStub PUBLISHER = MessagePublisherStub.create();

  private static final PipelinesWorkflow.Graph<StepType> WF =
      PipelinesWorkflow.getValidatorWorkflow();

  @Mock private PipelinesHistoryClient historyClient;

  @After
  public void after() {
    PUBLISHER.close();
  }

  @Test
  public void successInterpretationTest() {
    // State
    MetricsCollectorConfiguration config = createConfig("test-normal-case");
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, PUBLISHER, historyClient, validationClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    PipelinesIndexedMessage message = createMessage(uuid, attempt);

    // Index document
    String document =
        "{\"datasetKey\":\""
            + DATASET_UUID
            + "\",\"maximumElevationInMeters\":2.2,\"issues\":"
            + "[\"GEODETIC_DATUM_ASSUMED_WGS84\",\"LICENSE_MISSING_OR_UNKNOWN\"],\"verbatim\":{\"core\":"
            + "{\"http://rs.tdwg.org/dwc/terms/maximumElevationInMeters\":\"1150\","
            + "\"http://rs.tdwg.org/dwc/terms/organismID\":\"251\",\"http://rs.tdwg.org/dwc/terms/bed\":\"251\"},\"extensions\":"
            + "{\"http://rs.tdwg.org/dwc/terms/MeasurementOrFact\":[{\"http://rs.tdwg.org/dwc/terms/measurementValue\":"
            + "\"1.7\"},{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.0\"},"
            + "{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.83\"}]}}}";

    EsIndex.createIndex(
        ES_SERVER.getEsConfig(),
        IndexParams.builder()
            .indexName(config.indexName)
            .settingsType(INDEXING)
            .pathMappings(MAPPINGS_PATH)
            .build());

    EsService.indexDocument(ES_SERVER.getEsClient(), config.indexName, 1L, document);
    EsService.refreshIndex(ES_SERVER.getEsClient(), config.indexName);

    validationClient.update(
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
  public void successSingleInterpretationTest() {
    // State
    MetricsCollectorConfiguration config = createConfig("test-normal-single-step-case");
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, PUBLISHER, historyClient, validationClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;

    PipelinesIndexedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_COLLECT_METRICS.name()));

    // Index document
    String document =
        "{\"datasetKey\":\""
            + DATASET_UUID
            + "\",\"maximumElevationInMeters\":2.2,\"issues\":"
            + "[\"GEODETIC_DATUM_ASSUMED_WGS84\",\"RANDOM_ISSUE\"],\"verbatim\":{\"core\":"
            + "{\"http://rs.tdwg.org/dwc/terms/maximumElevationInMeters\":\"1150\","
            + "\"http://rs.tdwg.org/dwc/terms/organismID\":\"251\",\"http://rs.tdwg.org/dwc/terms/bed\":\"251\"},\"extensions\":"
            + "{\"http://rs.tdwg.org/dwc/terms/MeasurementOrFact\":[{\"http://rs.tdwg.org/dwc/terms/measurementValue\":"
            + "\"1.7\"},{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.0\"},"
            + "{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.83\"}]}}}";

    EsIndex.createIndex(
        ES_SERVER.getEsConfig(),
        IndexParams.builder()
            .indexName(config.indexName)
            .settingsType(INDEXING)
            .pathMappings(MAPPINGS_PATH)
            .build());

    EsService.indexDocument(ES_SERVER.getEsClient(), config.indexName, 1L, document);
    EsService.refreshIndex(ES_SERVER.getEsClient(), config.indexName);

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
    // State
    MetricsCollectorConfiguration config = createConfig("test-failed-case");
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
  public void successEventInterpretationTest() {
    // State
    MetricsCollectorConfiguration config = createConfig("test-event-normal-case");
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, PUBLISHER, historyClient, validationClient);

    String datasetUuid = "9997fa4e-54c1-43ea-9856-afa90204c162";
    UUID uuid = UUID.fromString(datasetUuid);
    int attempt = 60;

    PipelinesIndexedMessage message = createMessage(uuid, attempt);

    // Index document
    String document =
        "{\"datasetKey\":\""
            + datasetUuid
            + "\",\"maximumElevationInMeters\":2.2,\"issues\":"
            + "[\"GEODETIC_DATUM_ASSUMED_WGS84\",\"LICENSE_MISSING_OR_UNKNOWN\"],\"verbatim\":{\"core\":"
            + "{\"http://rs.tdwg.org/dwc/terms/maximumElevationInMeters\":\"1150\","
            + "\"http://rs.tdwg.org/dwc/terms/organismID\":\"251\",\"http://rs.tdwg.org/dwc/terms/bed\":\"251\"},\"extensions\":"
            + "{\"http://rs.tdwg.org/dwc/terms/MeasurementOrFact\":[{\"http://rs.tdwg.org/dwc/terms/measurementValue\":"
            + "\"1.7\"},{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.0\"},"
            + "{\"http://rs.tdwg.org/dwc/terms/measurementValue\":\"5.83\"}]}}}";

    EsIndex.createIndex(
        ES_SERVER.getEsConfig(),
        IndexParams.builder()
            .indexName(config.indexName)
            .settingsType(INDEXING)
            .pathMappings(MAPPINGS_PATH)
            .build());

    EsService.indexDocument(ES_SERVER.getEsClient(), config.indexName, 1L, document);
    EsService.refreshIndex(ES_SERVER.getEsClient(), config.indexName);

    validationClient.update(
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

  private MetricsCollectorConfiguration createConfig(String idxName) {
    MetricsCollectorConfiguration config = new MetricsCollectorConfiguration();
    // Main
    config.archiveRepository =
        this.getClass().getClassLoader().getResource("dataset/dwca").getPath();
    config.validatorOnly = true;

    // ES
    config.esConfig.hosts = ES_SERVER.getEsConfig().getRawHosts();
    // Step config
    config.stepConfig.repositoryPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    config.indexName = idxName;
    return config;
  }
}
