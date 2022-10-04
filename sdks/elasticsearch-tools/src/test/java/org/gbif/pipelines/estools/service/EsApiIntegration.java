package org.gbif.pipelines.estools.service;

import static org.gbif.pipelines.estools.service.EsService.buildEndpoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.gbif.pipelines.estools.service.EsConstants.Constant;
import org.gbif.pipelines.estools.service.EsConstants.Field;
import org.gbif.pipelines.estools.service.EsConstants.Indexing;
import org.gbif.pipelines.estools.service.EsConstants.Searching;
import org.junit.ClassRule;

/**
 * Base class to write JUnit integration tests with an embedded ES instance.
 *
 * <p>{@link ClassRule} requires this class to be public.
 */
public abstract class EsApiIntegration {

  /** {@link ClassRule} requires this field to be public. */
  @ClassRule public static final EsServer ES_SERVER = new EsServer();

  // files for testing
  static final Path TEST_MAPPINGS_PATH = Paths.get("mappings/simple-mapping.json");

  /** Utility method to get the settings of an index. */
  private static JsonNode getSettingsFromIndex(String idxName) {

    String endpoint = buildEndpoint(idxName, "_settings");
    try {
      RestClient client = ES_SERVER.getRestClient();
      Response response = client.performRequest(new Request(HttpGet.METHOD_NAME, endpoint));

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
    assertEquals(Searching.NUMBER_REPLICAS, indexSettings.path(Field.NUMBER_REPLICAS).asText());
  }

  /** Asserts that the swap operation was done as expected in the embedded ES instance. */
  static void assertSwapResults(
      String idxAdded, String idxPattern, String alias, Set<String> idxRemoved) {
    assertSwapResults(idxAdded, idxPattern, Collections.singleton(alias), idxRemoved);
  }

  /** Asserts that the swap operation was done as expected in the embedded ES instance. */
  static void assertSwapResults(
      String idxAdded, String idxPattern, Set<String> aliases, Set<String> idxRemoved) {

    aliases.forEach(
        alias -> {
          // get indexes of the alias again.
          Set<String> indexesFoundInAlias =
              EsService.getIndexesByAliasAndIndexPattern(
                  ES_SERVER.getEsClient(), idxPattern, alias);
          // Only the last index should be returned.
          assertEquals(1, indexesFoundInAlias.size());
          assertTrue(indexesFoundInAlias.contains(idxAdded));
        });

    // the other indexes shoudn't exist
    for (String removed : idxRemoved) {
      assertFalse(EsService.existsIndex(ES_SERVER.getEsClient(), removed));
    }
  }

  /** Utility method to add an index to an alias. */
  static void addIndexesToAlias(String alias, Set<String> idxToAdd) {
    addIndexesToAliases(Collections.singleton(alias), idxToAdd);
  }

  /** Utility method to add an index to some aliases. */
  static void addIndexesToAliases(Set<String> aliases, Set<String> idxToAdd) {
    // add them to the same alias
    HttpEntity entityBody =
        HttpRequestBuilder.newInstance()
            .withIndexAliasAction(aliases, idxToAdd, Collections.emptySet())
            .build();

    try {
      String endpoint = buildEndpoint("_aliases");
      RestClient client = ES_SERVER.getRestClient();

      Request request = new Request(HttpPost.METHOD_NAME, endpoint);
      request.setEntity(entityBody);
      // header
      RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
      optionsBuilder.addHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
      request.setOptions(optionsBuilder);

      client.performRequest(request);
    } catch (IOException e) {
      throw new AssertionError("Could not add indexes to alias", e);
    }
  }

  /** Utility method to get the mappings of an index. */
  static JsonNode getMappingsFromIndex(String idxName) {

    String endpoint = buildEndpoint(idxName, "_mapping");
    try {
      RestClient client = ES_SERVER.getRestClient();
      Response response = client.performRequest(new Request(HttpGet.METHOD_NAME, endpoint));

      // parse response and return
      return JsonHandler.readTree(response.getEntity());
    } catch (IOException e) {
      throw new AssertionError("Could not get the index mappings", e);
    }
  }
}
