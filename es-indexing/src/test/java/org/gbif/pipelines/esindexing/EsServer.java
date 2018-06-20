package org.gbif.pipelines.esindexing;

import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.client.EsConfig;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

/**
 * ES server used for testing purposes.
 *
 * <p>This class is intended to be used as a {@link org.junit.ClassRule}.
 */
public class EsServer extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(EsServer.class);

  private static EmbeddedElastic embeddedElastic;
  private static final String CLUSTER_NAME = "test_EScluster";
  private static EsConfig esConfig;
  // needed to assert results against ES server directly
  private static RestClient restClient;
  // I create both clients not to expose the restClient in EsClient
  private static EsClient esClient;

  @Override
  protected void before() throws Throwable {
    embeddedElastic =
        EmbeddedElastic.builder()
            // TODO: get version from pom??
            .withElasticVersion("5.6.3")
            .withEsJavaOpts("-Xms128m -Xmx512m")
            .withSetting(PopularProperties.HTTP_PORT, getAvailablePort())
            .withSetting(PopularProperties.TRANSPORT_TCP_PORT, getAvailablePort())
            .withSetting(PopularProperties.CLUSTER_NAME, CLUSTER_NAME)
            .build();

    embeddedElastic.start();

    esConfig = EsConfig.from(getServerAddress());
    restClient = buildRestClient();
    esClient = EsClient.from(esConfig);
  }

  @Override
  protected void after() {
    embeddedElastic.stop();
    esClient.close();
    try {
      restClient.close();
    } catch (IOException e) {
      LOG.error("Could not close rest client for testing", e);
    }
  }

  private static int getAvailablePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();

    return port;
  }

  private static String getServerAddress() {
    return "http://localhost:" + embeddedElastic.getHttpPort();
  }

  private static RestClient buildRestClient() {
    HttpHost host = new HttpHost("localhost", embeddedElastic.getHttpPort());
    return RestClient.builder(host).build();
  }

  public EsConfig getEsConfig() {
    return esConfig;
  }

  public RestClient getRestClient() {
    return restClient;
  }

  public EsClient getEsClient() {
    return esClient;
  }
}
