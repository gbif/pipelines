package org.gbif.pipelines.esindexing;

import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.client.EsConfig;
import org.gbif.pipelines.esindexing.request.EntityBuilder;
import org.gbif.pipelines.esindexing.response.ResponseParser;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import static org.gbif.pipelines.esindexing.common.EsConstants.DURABILITY_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEXING_NUMBER_REPLICAS;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEXING_REFRESH_INTERVAL;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.NUMBER_REPLICAS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.NUMBER_SHARDS;
import static org.gbif.pipelines.esindexing.common.EsConstants.NUMBER_SHARDS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.REFRESH_INTERVAL_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.SEARCHING_NUMBER_REPLICAS;
import static org.gbif.pipelines.esindexing.common.EsConstants.SEARCHING_REFRESH_INTERVAL;
import static org.gbif.pipelines.esindexing.common.EsConstants.SETTINGS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.TRANSLOG_DURABILITY;
import static org.gbif.pipelines.esindexing.common.EsConstants.TRANSLOG_FIELD;
import static org.gbif.pipelines.esindexing.common.JsonHandler.readTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class to write JUnit integration tests with an embedded ES instance.
 */
public class EsIntegrationTest {

  private static EmbeddedElastic embeddedElastic;
  private final static String CLUSTER_NAME = "test_EScluster";
  private static EsConfig esConfig;
  // needed to assert results against ES server directly
  private static RestClient restClient;
  // I create both clients not to expose the restClient in EsClient
  private static EsClient esClient;

  /**
   * Starts the embedded ES instance and creates all the necessary clients and configuration to be reused in the tests.
   */
  @BeforeClass
  public static void esSetup() throws IOException, InterruptedException {
    embeddedElastic = EmbeddedElastic.builder()
      // TODO: get version from pom??
      .withElasticVersion("5.6.0")
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

  /**
   * Stops the ES instance and closes the clients.
   */
  @AfterClass
  public static void esTearDown() throws IOException {
    embeddedElastic.stop();
    restClient.close();
    esClient.close();
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

  /**
   * Deletes all the indexes of the embedded ES instance.
   */
  protected static void deleteAllIndexes() {
    try {
      restClient.performRequest(HttpDelete.METHOD_NAME, "_all");
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  /**
   * Asserts that an index was created as expected in the embedded ES instance.
   */
  protected static Response assertCreatedIndex(String idx) {
    Response response = null;
    try {
      response = restClient.performRequest(HttpGet.METHOD_NAME, "/" + idx);
      assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

    return response;
  }

  /**
   * Asserts that the settings present in the response received for the index match with the default indexing
   * settings used in the library.
   */
  protected static void assertIndexingSettings(Response response, String idx) {
    JsonNode indexSettings = readTree(response.getEntity()).path(idx).path(SETTINGS_FIELD).path(INDEX_FIELD);
    assertEquals(INDEXING_REFRESH_INTERVAL, indexSettings.path(REFRESH_INTERVAL_FIELD).asText());
    assertEquals(NUMBER_SHARDS, indexSettings.path(NUMBER_SHARDS_FIELD).asText());
    assertEquals(INDEXING_NUMBER_REPLICAS, indexSettings.path(NUMBER_REPLICAS_FIELD).asText());
    assertEquals(TRANSLOG_DURABILITY, indexSettings.path(TRANSLOG_FIELD).path(DURABILITY_FIELD).asText());
  }

  /**
   * Asserts that the settings present in the response received for the index match with the default search
   * settings used in the library.
   */
  protected static void assertSearchSettings(Response response, String idx) {
    JsonNode indexSettings = readTree(response.getEntity()).path(idx).path(SETTINGS_FIELD).path(INDEX_FIELD);
    assertEquals(SEARCHING_REFRESH_INTERVAL, indexSettings.path(REFRESH_INTERVAL_FIELD).asText());
    assertEquals(NUMBER_SHARDS, indexSettings.path(NUMBER_SHARDS_FIELD).asText());
    assertEquals(SEARCHING_NUMBER_REPLICAS, indexSettings.path(NUMBER_REPLICAS_FIELD).asText());
    assertEquals(TRANSLOG_DURABILITY, indexSettings.path(TRANSLOG_FIELD).path(DURABILITY_FIELD).asText());
  }

  /**
   * Asserts that the swap operation was done as expected in the embedded ES instance.
   */
  protected static void assertSwapResults(String idxAdded, String idxPattern, String alias, Set<String> idxRemoved) {
    // get indexes of the alias again. Only the last index should be returned.
    Response response = null;
    try {
      response = restClient.performRequest(HttpGet.METHOD_NAME, "/" + idxPattern + "/_alias/" + alias);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

    // parse response
    Set<String> indexesFound = ResponseParser.parseIndexesInAliasResponse(response.getEntity());

    assertEquals(1, indexesFound.size());
    assertTrue(indexesFound.contains(idxAdded));

    // the other indexes shoudn't exist
    for (String removed : idxRemoved) {
      try {
        response = restClient.performRequest(HttpGet.METHOD_NAME, "/" + removed);
      } catch (ResponseException e) {
        assertEquals(HttpStatus.SC_NOT_FOUND, e.getResponse().getStatusLine().getStatusCode());
      } catch (IOException e) {
        throw new AssertionError(e);
      }
    }
  }

  /**
   * Utility method to add an index to an alias.
   */
  protected static void addIndexToAlias(String alias, Set<String> idxToAdd) {
    // add them to the same alias
    HttpEntity entityBody = EntityBuilder.entityIndexAliasActions(alias, idxToAdd, Collections.emptySet());

    try {
      restClient.performRequest(HttpPost.METHOD_NAME, "/_aliases", Collections.emptyMap(), entityBody);
    } catch (IOException e) {
      throw new AssertionError("Could not add indexes to alias", e);
    }
  }

}
