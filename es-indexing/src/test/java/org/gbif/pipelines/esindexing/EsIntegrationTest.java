package org.gbif.pipelines.esindexing;

import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.client.EsConfig;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

public class EsIntegrationTest {

  private static EmbeddedElastic embeddedElastic;
  private final static String CLUSTER_NAME = "test_EScluster";
  private static EsConfig esConfig;
  // needed to assert results against ES server directly
  private static RestClient restClient;
  // I create both clients not to expose the restClient in EsClient
  private static EsClient esClient;

  @BeforeClass
  public static void esSetup() throws IOException, InterruptedException {
    embeddedElastic = EmbeddedElastic.builder()
      // TODO: get version from pom
      .withElasticVersion("5.6.0")
      .withSetting(PopularProperties.HTTP_PORT, getAvailablePort())
      .withSetting(PopularProperties.TRANSPORT_TCP_PORT, getAvailablePort())
      .withSetting(PopularProperties.CLUSTER_NAME, CLUSTER_NAME)
      .build();

    embeddedElastic.start();

    esConfig = EsConfig.from(getServerAddress());
    restClient = buildRestClient();
    esClient = EsClient.from(esConfig);
  }

  @AfterClass
  public static void esTearDown() {
    embeddedElastic.stop();
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

  protected EsConfig getEsConfig() {
    return esConfig;
  }

  protected RestClient getRestClient() {
    return restClient;
  }

  protected EsClient getEsClient() {
    return esClient;
  }

}
