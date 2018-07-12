package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.EsServer;
import org.gbif.pipelines.esindexing.common.EsConstants.Constant;
import org.gbif.pipelines.esindexing.common.EsConstants.Field;
import org.gbif.pipelines.esindexing.common.EsConstants.Indexing;
import org.gbif.pipelines.esindexing.common.EsConstants.Searching;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base class to write JUnit integration tests with an embedded ES instance.
 *
 * <p>{@link ClassRule} requires this class to be public.
 */
public abstract class EsApiIntegrationTest {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final EsServer ES_SERVER = new EsServer();

  // files for testing
  static final Path TEST_MAPPINGS_PATH = Paths.get("mappings/simple-mapping.json");

  /** Utility method to get the settings of an index. */
  static JsonNode getSettingsFromIndex(String idx) {
    try {
      Response response =
          ES_SERVER
              .getRestClient()
              .performRequest(
                  HttpGet.METHOD_NAME,
                  EndpointHelper.getIndexSettingsEndpoint(idx),
                  Collections.emptyMap());

      // parse response and return
      return JsonHandler.readTree(response.getEntity());
    } catch (IOException e) {
      throw new AssertionError("Could not get the index mappings", e);
    }
  }

  /**
   * Asserts that the settings of the index match with the default indexing settings used in the
   * library.
   */
  static void assertIndexingSettings(String idx) {
    JsonNode indexSettings =
        getSettingsFromIndex(idx).path(idx).path(Field.SETTINGS).path(Field.INDEX);
    assertEquals(Indexing.REFRESH_INTERVAL, indexSettings.path(Field.REFRESH_INTERVAL).asText());
    assertEquals(Constant.NUMBER_SHARDS, indexSettings.path(Field.NUMBER_SHARDS).asText());
    assertEquals(Indexing.NUMBER_REPLICAS, indexSettings.path(Field.NUMBER_REPLICAS).asText());
    assertEquals(
        Constant.TRANSLOG_DURABILITY,
        indexSettings.path(Field.TRANSLOG).path(Field.DURABILITY).asText());
  }

  /**
   * Asserts that the settings of the index match with the default search settings used in the
   * library.
   */
  static void assertSearchSettings(String idx) {
    JsonNode indexSettings =
        getSettingsFromIndex(idx).path(idx).path(Field.SETTINGS).path(Field.INDEX);
    assertEquals(Searching.REFRESH_INTERVAL, indexSettings.path(Field.REFRESH_INTERVAL).asText());
    assertEquals(Constant.NUMBER_SHARDS, indexSettings.path(Field.NUMBER_SHARDS).asText());
    assertEquals(Searching.NUMBER_REPLICAS, indexSettings.path(Field.NUMBER_REPLICAS).asText());
    assertEquals(
        Constant.TRANSLOG_DURABILITY,
        indexSettings.path(Field.TRANSLOG).path(Field.DURABILITY).asText());
  }

  /** Asserts that the swap operation was done as expected in the embedded ES instance. */
  static void assertSwapResults(
      String idxAdded, String idxPattern, String alias, Set<String> idxRemoved) {
    // get indexes of the alias again.
    Set<String> indexesFoundInAlias =
        EsService.getIndexesByAliasAndIndexPattern(ES_SERVER.getEsClient(), idxPattern, alias);
    // Only the last index should be returned.
    assertEquals(1, indexesFoundInAlias.size());
    assertTrue(indexesFoundInAlias.contains(idxAdded));

    // the other indexes shoudn't exist
    for (String removed : idxRemoved) {
      assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), removed));
    }
  }

  /** Utility method to add an index to an alias. */
  static void addIndexesToAlias(String alias, Set<String> idxToAdd) {
    // add them to the same alias
    HttpEntity entityBody =
        BodyBuilder.newInstance()
            .withIndexAliasAction(alias, idxToAdd, Collections.emptySet())
            .build();

    try {
      ES_SERVER
          .getRestClient()
          .performRequest(
              HttpPost.METHOD_NAME,
              EndpointHelper.getAliasesEndpoint(),
              Collections.emptyMap(),
              entityBody);
    } catch (IOException e) {
      throw new AssertionError("Could not add indexes to alias", e);
    }
  }

  /** Utility method to get the mappings of an index. */
  static JsonNode getMappingsFromIndex(String idx) {
    try {
      Response response =
          ES_SERVER
              .getRestClient()
              .performRequest(
                  HttpGet.METHOD_NAME,
                  EndpointHelper.getIndexMappingsEndpoint(idx),
                  Collections.emptyMap());

      // parse response and return
      return JsonHandler.readTree(response.getEntity());
    } catch (IOException e) {
      throw new AssertionError("Could not get the index mappings", e);
    }
  }
}
