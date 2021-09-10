package org.gbif.pipelines.crawler.metrics;

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
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.messaging.api.messages.PipelinesIndexedMessage;
import org.gbif.crawler.constants.PipelinesNodePaths.Fn;
import org.gbif.pipelines.common.utils.ZookeeperUtils;
import org.gbif.pipelines.crawler.MessagePublisherStub;
import org.gbif.pipelines.crawler.utils.EsServer;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.registry.ws.client.pipelines.PipelinesHistoryClient;
import org.gbif.validator.api.EvaluationCategory;
import org.gbif.validator.api.Metrics.Core;
import org.gbif.validator.api.Metrics.Core.IssueInfo;
import org.gbif.validator.api.Metrics.Extension;
import org.gbif.validator.api.Validation;
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
    assertTrue(checkExists(curator, crawlId, LABEL));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_CLASS_NAME.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.MQ_MESSAGE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.END_DATE.apply(LABEL)));
    assertTrue(checkExists(curator, crawlId, Fn.SUCCESSFUL.apply(LABEL)));
    assertEquals(0, publisher.getMessages().size());

    Validation validation = validationClient.getValidation();

    Core core = validation.getMetrics().getCore();
    assertEquals("occurrence.txt", core.getFileName());
    assertEquals(Long.valueOf(1534L), core.getFileCount());
    assertEquals(Long.valueOf(1L), core.getIndexedCount());
    assertEquals(235, core.getIndexedCoreTerms().size());
    assertEquals(2, core.getOccurrenceIssues().size());

    Optional<IssueInfo> randomIssue =
        core.getOccurrenceIssues().stream()
            .filter(x -> x.getIssue().equals("RANDOM_ISSUE"))
            .findAny();
    assertTrue(randomIssue.isPresent());
    assertEquals(Long.valueOf(1), randomIssue.get().getCount());
    assertNull(randomIssue.get().getIssueCategory());

    Optional<IssueInfo> geodeticDatumAssumedWgs84Issue =
        core.getOccurrenceIssues().stream()
            .filter(x -> x.getIssue().equals(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84.name()))
            .findAny();
    assertTrue(geodeticDatumAssumedWgs84Issue.isPresent());
    assertEquals(Long.valueOf(1), geodeticDatumAssumedWgs84Issue.get().getCount());
    assertEquals(
        EvaluationCategory.OCC_INTERPRETATION_BASED,
        geodeticDatumAssumedWgs84Issue.get().getIssueCategory());

    assertEquals(1L, validation.getMetrics().getExtensions().size());

    Extension extension = validation.getMetrics().getExtensions().get(0);
    assertEquals("multimedia.txt", extension.getFileName());

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
