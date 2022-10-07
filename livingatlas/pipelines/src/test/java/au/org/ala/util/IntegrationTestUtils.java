package au.org.ala.util;

import static au.org.ala.util.TestUtils.ES_INTERNAL_PORT;
import static au.org.ala.util.TestUtils.NAME_SERVICE_IMG;
import static au.org.ala.util.TestUtils.NAME_SERVICE_INTERNAL_PORT;
import static au.org.ala.util.TestUtils.SENSITIVE_SERVICE_INTERNAL_PORT;
import static au.org.ala.util.TestUtils.SENSTIVE_SERVICE_IMG;
import static au.org.ala.util.TestUtils.SOLR_IMG;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.utils.ALAFsUtils;
import okhttp3.mockwebserver.MockWebServer;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/** Singleton that sets up docker instances and mock web services to support integration tests. */
public final class IntegrationTestUtils {

  private static IntegrationTestUtils INSTANCE;
  private boolean isSetup = false;

  GenericContainer nameService;
  GenericContainer sdsService;
  GenericContainer solrService;
  ElasticsearchContainer elasticsearchContainer;
  MockWebServer server;
  MockWebServer speciesListServer;
  MockWebServer samplingServer;
  String propertiesFilePath;
  ALAPipelinesConfig config;

  private IntegrationTestUtils() {}

  public static IntegrationTestUtils getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new IntegrationTestUtils();
    }
    return INSTANCE;
  }

  public void setup() {

    if (!isSetup) {

      // setup containers
      int[] solrPorts = TestUtils.getFreePortsForSolr();
      int zkPort = solrPorts[1];
      int solrPort = solrPorts[0];

      solrService =
          new FixedHostPortGenericContainer(SOLR_IMG)
              .withFixedExposedPort(zkPort, zkPort)
              .withFixedExposedPort(solrPort, solrPort)
              .withCommand("-c -p " + solrPort);
      solrService.start();

      nameService =
          new GenericContainer(DockerImageName.parse(NAME_SERVICE_IMG))
              .withExposedPorts(NAME_SERVICE_INTERNAL_PORT);
      nameService.start();

      sdsService =
          new GenericContainer(DockerImageName.parse(SENSTIVE_SERVICE_IMG))
              .withExposedPorts(SENSITIVE_SERVICE_INTERNAL_PORT);
      sdsService.start();

      elasticsearchContainer =
          new ElasticsearchContainer(
                  DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
                      .withTag("7.10.2"))
              .withReuse(true);
      elasticsearchContainer.start();

      TestUtils.setSolrPorts(solrPort, zkPort);
      TestUtils.setNameServicePort(nameService.getMappedPort(NAME_SERVICE_INTERNAL_PORT));
      TestUtils.setSDSPort(sdsService.getMappedPort(SENSITIVE_SERVICE_INTERNAL_PORT));
      TestUtils.setESPort(elasticsearchContainer.getMappedPort(ES_INTERNAL_PORT));

      server = TestUtils.startMockCollectoryServer();
      speciesListServer = TestUtils.startSpeciesListServer();
      samplingServer = TestUtils.startMockSpatialServer();

      propertiesFilePath = TestUtils.getPipelinesConfigFile();

      config = ALAFsUtils.readConfigFile(HdfsConfigs.nullConfig(), propertiesFilePath);

      isSetup = true;
    }
  }

  public String getPropertiesFilePath() {
    if (propertiesFilePath == null) {
      throw new RuntimeException("Run setup first !");
    }
    return propertiesFilePath;
  }

  public ALAPipelinesConfig getConfig() {
    if (config == null) {
      throw new RuntimeException("Run setup first !");
    }
    return config;
  }
}
