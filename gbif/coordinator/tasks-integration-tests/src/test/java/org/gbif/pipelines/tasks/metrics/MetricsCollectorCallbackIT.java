package org.gbif.pipelines.tasks.metrics;

import static org.gbif.crawler.constants.PipelinesNodePaths.getPipelinesInfoPath;
import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.UUID;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.gbif.api.model.pipelines.StepRunner;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.tasks.MessagePublisherStub;
import org.gbif.pipelines.tasks.ValidationWsClientStub;
import org.gbif.pipelines.tasks.utils.EsServer;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

public class MetricsCollectorCallbackIT {

  private static final String LABEL = StepType.VALIDATOR_COLLECT_METRICS.getLabel();
  private static final String DATASET_UUID = "9bed66b3-4caa-42bb-9c93-71d7ba109dad";
  private static final long EXECUTION_ID = 1L;
  private static final Path MAPPINGS_PATH = Paths.get("mappings/verbatim-mapping.json");
  private static CuratorFramework curator;
  private static TestingServer server;
  private static MessagePublisherStub publisher;
  private static PipelinesHistoryClient historyClient;

  @ClassRule public static final EsServer ES_SERVER = new EsServer();

  @Before
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

  @BeforeClass
  public static void setUp() throws Exception {

    server = new TestingServer();
    curator =
        CuratorFrameworkFactory.builder()
            .connectString(server.getConnectString())
            .namespace("crawler")
            .retryPolicy(new RetryOneTime(1))
            .build();
    curator.start();

    publisher = MessagePublisherStub.create();

    historyClient = Mockito.mock(PipelinesHistoryClient.class);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    curator.close();
    server.stop();
    publisher.close();
  }

  @After
  public void after() {
    publisher.close();
  }

  @Test
  public void testNormalCase() throws Exception {
    // State
    MetricsCollectorConfiguration config = createConfig();
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, publisher, curator, historyClient, validationClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;

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
                                .stepType(StepType.VALIDATOR_COLLECT_METRICS.name())
                                .build(),
                            ValidationStep.builder()
                                .stepType(StepType.VALIDATOR_VALIDATE_ARCHIVE.name())
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
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.END_DATE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL.apply(LABEL)));
    assertEquals(0, publisher.getMessages().size());

    Validation validation = validationClient.getValidation();

    assertEquals(2, validation.getMetrics().getFileInfos().size());
    assertEquals(Status.QUEUED, validation.getStatus());
    assertFalse(validation.getMetrics().isIndexeable());

    Optional<FileInfo> coreOpt = validationClient.getFileInfo(DwcTerm.Occurrence);
    assertTrue(coreOpt.isPresent());

    FileInfo core = coreOpt.get();
    assertEquals("verbatim.txt", core.getFileName());
    assertEquals(Long.valueOf(1534L), core.getCount());
    assertEquals(Long.valueOf(1L), core.getIndexedCount());
    assertEquals(235, core.getTerms().size());
    assertEquals(3, core.getIssues().size());
    assertEquals(DwcFileType.CORE, core.getFileType());

    Optional<IssueInfo> oldIssue =
        core.getIssues().stream().filter(x -> x.getIssue().equals("OLD")).findAny();
    assertTrue(oldIssue.isPresent());
    assertEquals(Long.valueOf(999), oldIssue.get().getCount());
    assertNull(oldIssue.get().getIssueCategory());

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

    Optional<FileInfo> extOpt = validationClient.getFileInfo(Extension.MULTIMEDIA.getRowType());
    assertTrue(extOpt.isPresent());

    FileInfo ext = extOpt.get();

    assertEquals("multimedia.txt", ext.getFileName());
    assertEquals(DwcFileType.EXTENSION, ext.getFileType());

    // Clean
    curator.delete().deletingChildrenIfNeeded().forPath(getPipelinesInfoPath(crawlId, LABEL, true));
  }

  @Test
  public void testNormalSingleStepCase() {
    // State
    MetricsCollectorConfiguration config = createConfig();
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, publisher, curator, historyClient, validationClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;

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
    assertFalse(checkExists(curator, crawlId, LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.END_DATE.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.SUCCESSFUL.apply(LABEL)));
    assertEquals(0, publisher.getMessages().size());

    Validation validation = validationClient.getValidation();

    assertEquals(2, validation.getMetrics().getFileInfos().size());
    assertEquals(Status.FINISHED, validation.getStatus());
    assertTrue(validation.getMetrics().isIndexeable());
  }

  @Test
  public void testFailedCase() {
    // State
    MetricsCollectorConfiguration config = createConfig();
    ValidationWsClientStub validationClient = ValidationWsClientStub.create();

    MetricsCollectorCallback callback =
        new MetricsCollectorCallback(config, publisher, curator, historyClient, validationClient);

    UUID uuid = UUID.fromString(DATASET_UUID);
    int attempt = 60;
    String crawlId = DATASET_UUID;

    PipelinesIndexedMessage message = createMessage(uuid, attempt);
    message.setPipelineSteps(Collections.singleton(StepType.VALIDATOR_COLLECT_METRICS.name()));

    // When
    callback.handleMessage(message);

    // Should
    assertFalse(checkExists(curator, crawlId, LABEL));
    assertFalse(checkExists(curator, crawlId, Fn.ERROR_MESSAGE.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertFalse(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(publisher.getMessages().isEmpty());
  }

  private PipelinesIndexedMessage createMessage(UUID uuid, int attempt) {
    PipelinesIndexedMessage message = new PipelinesIndexedMessage();

    message.setDatasetUuid(uuid);
    message.setAttempt(attempt);
    message.setExecutionId(EXECUTION_ID);
    message.setValidator(true);
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

    // ES
    config.esConfig.hosts = ES_SERVER.getEsConfig().getRawHosts();
    // Step config
    config.stepConfig.repositoryPath =
        this.getClass().getClassLoader().getResource("data7/ingest").getPath();
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    return config;
  }

  private boolean checkExists(CuratorFramework curator, String id, String path) {
    return ZookeeperUtils.checkExists(curator, getPipelinesInfoPath(id, path, true));
  }
}
