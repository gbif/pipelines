package org.gbif.pipelines;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.client.EsConfig;
import org.junit.rules.ExternalResource;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * ES server used for testing purposes.
 *
 * <p>This class is intended to be used as a {@link org.junit.ClassRule}.
 */
@Slf4j
@Getter
public class EsServer extends ExternalResource {

  private static final Object MUTEX = new Object();
  private static volatile EsServer instance;
  private static final AtomicInteger COUNTER = new AtomicInteger(0);

  private ElasticsearchContainer embeddedElastic;
  private EsConfig esConfig;
  private RestClient restClient;
  private EsClient esClient;

  public static EsServer getInstance() {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new EsServer();
        }
      }
    }
    return instance;
  }

  @Override
  protected void before() throws Throwable {
    if (COUNTER.get() == 0) {
      embeddedElastic =
          new ElasticsearchContainer(
                  DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch-oss")
                      .withTag(getEsVersion()))
              .withReuse(true);

      embeddedElastic.start();

      esConfig = EsConfig.from(getServerAddress());
      restClient = buildRestClient();
      esClient = EsClient.from(esConfig);

      // Fix for https://github.com/gbif/pipelines/issues/568
      esClient.performPutRequest(
          "/_cluster/settings",
          Collections.emptyMap(),
          new NStringEntity(
              "{\"persistent\":{\"cluster.routing.allocation.disk.threshold_enabled\":false}}"));
    }
  }

  @Override
  protected void after() {
    if (COUNTER.addAndGet(-1) == 0) {
      embeddedElastic.stop();
      esClient.close();
      try {
        restClient.close();
      } catch (IOException e) {
        log.error("Could not close rest client for testing", e);
      }
    }
  }

  private RestClient buildRestClient() {
    HttpHost host = new HttpHost("localhost", embeddedElastic.getMappedPort(9200));
    return RestClient.builder(host).build();
  }

  public String getServerAddress() {
    return "http://localhost:" + embeddedElastic.getMappedPort(9200);
  }

  private String getEsVersion() throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getClassLoader().getResourceAsStream("maven.properties"));
    return properties.getProperty("elasticsearch.version");
  }
}
