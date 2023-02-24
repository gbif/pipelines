package org.gbif.pipelines.tasks.validators.cleaner;

import static org.gbif.pipelines.estools.common.SettingsType.INDEXING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import org.gbif.common.messaging.api.messages.PipelinesCleanerMessage;
import org.gbif.pipelines.estools.EsIndex;
import org.gbif.pipelines.estools.model.IndexParams;
import org.gbif.pipelines.estools.service.EsService;
import org.gbif.pipelines.tasks.ValidationWsClientStub;
import org.gbif.pipelines.tasks.resources.EsServer;
import org.gbif.validator.ws.client.ValidationWsClient;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class CleanerCallbackIT {

  @ClassRule public static final EsServer ES_SERVER = EsServer.getInstance();

  @Before
  public void cleanIndexes() {
    EsService.deleteAllIndexes(ES_SERVER.getEsClient());
  }

  @Test
  public void cleanerDeleteEsRecordsTest() {

    // State
    String datasetUuid = "8a4934ac-7d7f-41d4-892c-f6b71bb777a3";
    CleanerConfiguration config = createConfig();
    PipelinesCleanerMessage message = createMessage(datasetUuid);
    ValidationWsClient validationClient = ValidationWsClientStub.create();

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
            .indexName(config.esAliases[0])
            .settingsType(INDEXING)
            .pathMappings(Paths.get("mappings/verbatim-mapping.json"))
            .build());

    EsService.indexDocument(ES_SERVER.getEsClient(), config.esAliases[0], 1L, document);
    EsService.refreshIndex(ES_SERVER.getEsClient(), config.esAliases[0]);

    // When
    new CleanerCallback(config, validationClient).handleMessage(message);

    // Update deleted data available
    EsService.refreshIndex(ES_SERVER.getEsClient(), config.esAliases[0]);

    // Should
    assertFalse(Files.exists(Paths.get(String.join("/", config.fsRootPath, datasetUuid))));
    assertFalse(Files.exists(Paths.get(String.join("/", config.hdfsRootPath, datasetUuid))));
    assertEquals(0L, EsService.countIndexDocuments(ES_SERVER.getEsClient(), config.esAliases[0]));
    assertNotNull(validationClient.get(UUID.fromString(datasetUuid)).getDeleted());
  }

  @Test
  public void cleanerDeleteEsIndexTest() {

    // State
    String datasetUuid = "8a4934ac-7d7f-41d4-892c-f6b71bb777a3";
    CleanerConfiguration config = createConfig();
    PipelinesCleanerMessage message = createMessage(datasetUuid);
    ValidationWsClient validationClient = ValidationWsClientStub.create();

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

    String indexName = datasetUuid + "_vld_123123";
    String indexToSwap = datasetUuid + "_vld_777777";

    EsIndex.createIndex(
        ES_SERVER.getEsConfig(),
        IndexParams.builder()
            .indexName(indexName)
            .settingsType(INDEXING)
            .pathMappings(Paths.get("mappings/verbatim-mapping.json"))
            .build());

    EsIndex.createIndex(
        ES_SERVER.getEsConfig(),
        IndexParams.builder()
            .indexName(indexToSwap)
            .settingsType(INDEXING)
            .pathMappings(Paths.get("mappings/verbatim-mapping.json"))
            .build());

    EsService.indexDocument(ES_SERVER.getEsClient(), indexName, 1L, document);
    EsService.refreshIndex(ES_SERVER.getEsClient(), indexName);
    EsService.swapIndexes(
        ES_SERVER.getEsClient(),
        new HashSet<>(Arrays.asList(config.esAliases)),
        Collections.singleton(indexName),
        Collections.singleton(indexToSwap));

    // When
    new CleanerCallback(config, validationClient).handleMessage(message);

    // Should
    assertFalse(Files.exists(Paths.get(String.join("/", config.fsRootPath, datasetUuid))));
    assertFalse(Files.exists(Paths.get(String.join("/", config.hdfsRootPath, datasetUuid))));
    assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), indexName));
    assertNotNull(validationClient.get(UUID.fromString(datasetUuid)).getDeleted());
  }

  private PipelinesCleanerMessage createMessage(String datasetUuid) {
    PipelinesCleanerMessage message = new PipelinesCleanerMessage();
    message.setDatasetUuid(UUID.fromString(datasetUuid));
    message.setAttempt(1);
    message.setValidator(true);
    return message;
  }

  private CleanerConfiguration createConfig() {
    CleanerConfiguration config = new CleanerConfiguration();
    // ES
    config.esHosts = ES_SERVER.getEsConfig().getRawHosts();
    config.esAliases = new String[] {"validator"};
    //
    config.fsRootPath = getClass().getResource("/cleaner/fs").getPath();
    config.hdfsRootPath = getClass().getResource("/cleaner/hdfs").getPath();

    // Step config
    config.stepConfig.coreSiteConfig = "";
    config.stepConfig.hdfsSiteConfig = "";
    return config;
  }
}
