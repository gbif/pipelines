package org.gbif.pipelines.esindexing;

import org.gbif.pipelines.esindexing.api.EndpointHelper;
import org.gbif.pipelines.esindexing.common.JsonHandler;
import org.gbif.pipelines.esindexing.request.BodyBuilder;
import org.gbif.pipelines.esindexing.response.ResponseParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.Assert;
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
import static org.gbif.pipelines.esindexing.common.JsonHandler.readTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class to write JUnit integration tests with an embedded ES instance.
 */
public class EsIntegrationTest {

  @ClassRule
  public static EsServer esServer = new EsServer();

  // files for testing
  protected static final String TEST_MAPPINGS_PATH = "mappings/simple-mapping.json";

  /**
   * Deletes all the indexes of the embedded ES instance.
   */
  protected static void deleteAllIndexes() {
    try {
      esServer.getRestClient().performRequest(HttpDelete.METHOD_NAME, "_all");
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
      response = esServer.getRestClient().performRequest(HttpGet.METHOD_NAME, EndpointHelper.getIndexEndpoint(idx));
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
      response =
        esServer.getRestClient().performRequest(HttpGet.METHOD_NAME, EndpointHelper.getAliasIndexexEndpoint(idxPattern, alias));
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
        response = esServer.getRestClient().performRequest(HttpGet.METHOD_NAME, EndpointHelper.getIndexEndpoint(removed));
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
    HttpEntity entityBody =
      BodyBuilder.newInstance().withIndexAliasAction(alias, idxToAdd, Collections.emptySet()).build();

    try {
      esServer.getRestClient().performRequest(HttpPost.METHOD_NAME,
                                EndpointHelper.getAliasesEndpoint(),
                                Collections.emptyMap(),
                                entityBody);
    } catch (IOException e) {
      throw new AssertionError("Could not add indexes to alias", e);
    }
  }

  /**
   * Utility method to get the mappings of an index.
   */
  protected static JsonNode getMappingsFromIndex(String idx) {
    try {
      Response response = esServer.getRestClient().performRequest(HttpGet.METHOD_NAME,
                                                    EndpointHelper.getIndexMappingsEndpoint(idx),
                                                    Collections.emptyMap());

      // parse response and return
      return JsonHandler.readTree(response.getEntity());
    } catch (IOException e) {
      throw new AssertionError("Could not get the index mappings", e);
    }
  }

}
