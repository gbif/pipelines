package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.EsServer;
import org.gbif.pipelines.esindexing.common.JsonHandler;
import org.gbif.pipelines.esindexing.request.BodyBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Response;
import org.junit.ClassRule;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base class to write JUnit integration tests with an embedded ES instance.
 *
 * {@link ClassRule} requires this class to be public.
 */
public abstract class EsApiIntegrationTest {

  /**
   * {@link ClassRule} requires this field to be public.
   */
  @ClassRule
  public static EsServer esServer = new EsServer();

  // files for testing
  static final Path TEST_MAPPINGS_PATH = Paths.get("mappings/simple-mapping.json");

  /**
   * Utility method to get the settings of an index.
   */
  static JsonNode getSettingsFromIndex(String idx) {
    try {
      Response response = esServer.getRestClient()
        .performRequest(HttpGet.METHOD_NAME, EndpointHelper.getIndexSettingsEndpoint(idx), Collections.emptyMap());

      // parse response and return
      return JsonHandler.readTree(response.getEntity());
    } catch (IOException e) {
      throw new AssertionError("Could not get the index mappings", e);
    }
  }

  /**
   * Asserts that the settings of the index match with the default indexing settings used in the library.
   */
  static void assertIndexingSettings(String idx) {
    JsonNode indexSettings = getSettingsFromIndex(idx).path(idx).path(SETTINGS_FIELD).path(INDEX_FIELD);
    assertEquals(INDEXING_REFRESH_INTERVAL, indexSettings.path(REFRESH_INTERVAL_FIELD).asText());
    assertEquals(NUMBER_SHARDS, indexSettings.path(NUMBER_SHARDS_FIELD).asText());
    assertEquals(INDEXING_NUMBER_REPLICAS, indexSettings.path(NUMBER_REPLICAS_FIELD).asText());
    assertEquals(TRANSLOG_DURABILITY, indexSettings.path(TRANSLOG_FIELD).path(DURABILITY_FIELD).asText());
  }

  /**
   * Asserts that the settings of the index match with the default search settings used in the library.
   */
  static void assertSearchSettings(String idx) {
    JsonNode indexSettings = getSettingsFromIndex(idx).path(idx).path(SETTINGS_FIELD).path(INDEX_FIELD);
    assertEquals(SEARCHING_REFRESH_INTERVAL, indexSettings.path(REFRESH_INTERVAL_FIELD).asText());
    assertEquals(NUMBER_SHARDS, indexSettings.path(NUMBER_SHARDS_FIELD).asText());
    assertEquals(SEARCHING_NUMBER_REPLICAS, indexSettings.path(NUMBER_REPLICAS_FIELD).asText());
    assertEquals(TRANSLOG_DURABILITY, indexSettings.path(TRANSLOG_FIELD).path(DURABILITY_FIELD).asText());
  }

  /**
   * Asserts that the swap operation was done as expected in the embedded ES instance.
   */
  static void assertSwapResults(String idxAdded, String idxPattern, String alias, Set<String> idxRemoved) {
    // get indexes of the alias again.
    Set<String> indexesFoundInAlias =
      EsService.getIndexesByAliasAndIndexPattern(esServer.getEsClient(), idxPattern, alias);
    // Only the last index should be returned.
    assertEquals(1, indexesFoundInAlias.size());
    assertTrue(indexesFoundInAlias.contains(idxAdded));

    // the other indexes shoudn't exist
    for (String removed : idxRemoved) {
      assertFalse(EsService.existsIndex(esServer.getEsClient(), removed));
    }
  }

  /**
   * Utility method to add an index to an alias.
   */
  static void addIndexesToAlias(String alias, Set<String> idxToAdd) {
    // add them to the same alias
    HttpEntity entityBody =
      BodyBuilder.newInstance().withIndexAliasAction(alias, idxToAdd, Collections.emptySet()).build();

    try {
      esServer.getRestClient()
        .performRequest(HttpPost.METHOD_NAME, EndpointHelper.getAliasesEndpoint(), Collections.emptyMap(), entityBody);
    } catch (IOException e) {
      throw new AssertionError("Could not add indexes to alias", e);
    }
  }

  /**
   * Utility method to get the mappings of an index.
   */
  static JsonNode getMappingsFromIndex(String idx) {
    try {
      Response response = esServer.getRestClient()
        .performRequest(HttpGet.METHOD_NAME, EndpointHelper.getIndexMappingsEndpoint(idx), Collections.emptyMap());

      // parse response and return
      return JsonHandler.readTree(response.getEntity());
    } catch (IOException e) {
      throw new AssertionError("Could not get the index mappings", e);
    }
  }

}
