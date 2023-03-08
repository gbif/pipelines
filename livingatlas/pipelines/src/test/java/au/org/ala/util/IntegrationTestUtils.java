package au.org.ala.util;

import static au.org.ala.util.TestUtils.ES_INTERNAL_PORT;
import static au.org.ala.util.TestUtils.NAME_SERVICE_IMG;
import static au.org.ala.util.TestUtils.NAME_SERVICE_INTERNAL_PORT;
import static au.org.ala.util.TestUtils.SENSITIVE_SERVICE_INTERNAL_PORT;
import static au.org.ala.util.TestUtils.SENSTIVE_SERVICE_IMG;
import static au.org.ala.util.TestUtils.SOLR_IMG;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.utils.ALAFsUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.http.nio.entity.NStringEntity;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;
import org.junit.rules.ExternalResource;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/** Singleton that sets up docker instances and mock web services to support integration tests. */
@Slf4j
public class IntegrationTestUtils extends ExternalResource {

  private static IntegrationTestUtils INSTANCE;
  private static final Object MUTEX = new Object();
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

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
      synchronized (MUTEX) {
        if (INSTANCE == null) {
          INSTANCE = new IntegrationTestUtils();
        }
      }
    }
    return INSTANCE;
  }

  @Override
  protected void before() throws Throwable {

    if (COUNTER.get() == 0) {

      // setup containers
      int[] solrPorts = TestUtils.getFreePortsForSolr();
      int zkPort = solrPorts[1];
      int solrPort = solrPorts[0];

      solrService =
          new FixedHostPortGenericContainer(SOLR_IMG)
              .withFixedExposedPort(zkPort, zkPort)
              .withFixedExposedPort(solrPort, solrPort)
              .withEnv("SOLR_PORT", solrPort + "")
              .withEnv("ZOO_PORT", zkPort + "")
              .withEnv("ZOO_HOST", "localhost")
              .withEnv("SOLR_HOST", "localhost")
              .withEnv("SOLR_MODE", "solrcloud");
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

      // Fix for https://github.com/gbif/pipelines/issues/568
      try (EsClient esClient =
          EsClient.from(
              EsConfig.from(
                  "http://localhost:" + elasticsearchContainer.getMappedPort(ES_INTERNAL_PORT)))) {
        esClient.performPutRequest(
            "/_cluster/settings",
            Collections.emptyMap(),
            new NStringEntity(
                "{\"persistent\":{\"cluster.routing.allocation.disk.threshold_enabled\":false}}"));
      }
    }
  }

  @Override
  protected void after() {
    if (COUNTER.addAndGet(-1) == 0) {
      elasticsearchContainer.stop();
      solrService.close();
      sdsService.close();
      nameService.close();
      try {
        speciesListServer.close();
      } catch (IOException e) {
        log.error("Could not close rest client for testing", e);
      }
      try {
        server.close();
      } catch (IOException e) {
        log.error("Could not close rest client for testing", e);
      }
      try {
        samplingServer.close();
      } catch (IOException e) {
        log.error("Could not close rest client for testing", e);
      }
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
