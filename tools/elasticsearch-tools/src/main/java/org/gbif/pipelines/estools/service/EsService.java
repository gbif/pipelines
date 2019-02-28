package org.gbif.pipelines.estools.service;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.common.SettingsType;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.pipelines.estools.service.HttpRequestBuilder.createBodyFromString;

/**
 * Service to perform ES operations.
 *
 * <p>The {@link EsClient} is always received as a parameter and this class has no responsibility on
 * handling the connection with the ES server.
 *
 * <p>
 *
 * <p>This class is intended to be used internally within the same package, and <strong>never as a
 * public API</strong>. Therefore, the access modifiers should never be changed.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EsService {

  /**
   * Creates a ES index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to create.
   * @param settingsType settings to use in the call.
   * @return name of the index created.
   */
  public static String createIndex(@NonNull EsClient esClient, String idxName, SettingsType settingsType) {
    // create entity body
    HttpEntity body = HttpRequestBuilder.newInstance().withSettingsType(settingsType).build();

    return createIndexInternal(esClient, idxName, body);
  }

  /**
   * Creates a ES index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to create.
   * @param settingsType settings to use in the call.
   * @param mappings path of the file with the mappings.
   * @return name of the index created.
   */
  public static String createIndex(
      @NonNull EsClient esClient, String idxName, SettingsType settingsType, Path mappings) {

    // create entity body
    HttpEntity body =
        HttpRequestBuilder.newInstance()
            .withSettingsType(settingsType)
            .withMappings(mappings)
            .build();

    return createIndexInternal(esClient, idxName, body);
  }

  /**
   * Creates a ES index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to create.
   * @param settingsType settings to use in the call.
   * @param mappings mappings as json.
   * @return name of the index created.
   */
  public static String createIndex(
      @NonNull EsClient esClient, String idxName, SettingsType settingsType, String mappings) {

    // create entity body
    HttpEntity body =
        HttpRequestBuilder.newInstance()
            .withSettingsType(settingsType)
            .withMappings(mappings)
            .build();

    return createIndexInternal(esClient, idxName, body);
  }

  /**
   * Creates a ES index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to create.
   * @param settingsType settings to use in the call.
   * @param mappings mappings as json.
   * @param settings custom settings, number of shards and etc.
   * @return name of the index created.
   */
  public static String createIndex(
      @NonNull EsClient esClient,
      String idxName,
      SettingsType settingsType,
      Path mappings,
      Map<String, String> settings) {

    // create entity body
    HttpEntity body =
        HttpRequestBuilder.newInstance()
            .withSettingsType(settingsType)
            .withSettingsMap(settings)
            .withMappings(mappings)
            .build();

    return createIndexInternal(esClient, idxName, body);
  }

  /**
   * Creates a ES index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to create.
   * @param settings {@link Map} with thesettings to use in the call.
   * @param mappings path of the file with the mappings.
   * @return name of the index created.
   */
  public static String createIndex(
      @NonNull EsClient esClient, String idxName, Map<String, String> settings, Path mappings) {

    // create entity body
    HttpEntity body = HttpRequestBuilder.newInstance().withSettingsMap(settings).withMappings(mappings).build();

    return createIndexInternal(esClient, idxName, body);
  }

  @SneakyThrows
  private static String createIndexInternal(@NonNull EsClient esClient, String idxName, HttpEntity body) {
    String endpoint = buildEndpoint(idxName);
    Response response = esClient.performPutRequest(endpoint, Collections.emptyMap(), body);
    // parse response and return
    return HttpResponseParser.parseCreatedIndexResponse(response.getEntity());
  }

  /**
   * Updates the settings of an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to update.
   * @param settingsType settings that will be set to the index.
   */
  @SneakyThrows
  public static void updateIndexSettings(@NonNull EsClient esClient, String idxName, SettingsType settingsType) {

    // create entity body with settings
    HttpEntity body = HttpRequestBuilder.newInstance().withSettingsType(settingsType).build();

    String endpoint = buildEndpoint(idxName, "_settings");
    esClient.performPutRequest(endpoint, Collections.emptyMap(), body);
  }

  /**
   * Gets all the indexes associated to a specific alias and whose names match with a specified
   * pattern.
   *
   * @param esClient client to call ES. It is required.
   * @param idxPattern index to pattern. It can be the exact name of an index to do the query for a
   * single index, or a pattern using wildcards. For example, "idx*" matches with all the
   * indexes whose name starts with "idx".
   * @param alias alias that has to be associated to the indexes retrieved.
   * @return {@link Set} with all the indexes that are in the alias specified and match with the
   * pattern received.
   */
  public static Set<String> getIndexesByAliasAndIndexPattern(
      @NonNull EsClient esClient, String idxPattern, String alias) {

    String endpoint = buildEndpoint(idxPattern, "_alias", alias);
    try {
      Response response = esClient.performGetRequest(endpoint);
      return HttpResponseParser.parseIndexesInAliasResponse(response.getEntity());
    } catch (ResponseException e) {
      log.debug("No indexes with pattern {} found in alias {}", idxPattern, alias);
      return Collections.emptySet();
    }
  }

  /**
   * Swaps indexes in an alias.
   *
   * <p>In this method we can add or remove indexes in an alias. Also note that in the case of
   * removing indixes, they are <strong>completely removed</strong> from the ES instance, and not
   * only from the alias.
   *
   * @param esClient client to call ES. It is required.
   * @param alias alias that will be modified
   * @param idxToAdd indexes to add to the alias.
   * @param idxToRemove indexes to remove from the alias.
   */
  @SneakyThrows
  public static void swapIndexes(
      @NonNull EsClient esClient, String alias, Set<String> idxToAdd, Set<String> idxToRemove) {

    HttpEntity body = HttpRequestBuilder.newInstance().withIndexAliasAction(alias, idxToAdd, idxToRemove).build();
    String endpoint = buildEndpoint("_aliases");
    esClient.performPostRequest(endpoint, Collections.emptyMap(), body);
  }

  /**
   * Counts the number of documents of an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName index to get the count from.
   * @return number of documents of the index.
   */
  @SneakyThrows
  public static long countIndexDocuments(@NonNull EsClient esClient, String idxName) {
    String endpoint = buildEndpoint(idxName, "_count/");
    Response response = esClient.performGetRequest(endpoint);
    return HttpResponseParser.parseIndexCountResponse(response.getEntity());
  }

  /**
   * Indexes a document in an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName index where the document has to be indexed to.
   * @param type type of the document.
   * @param id id of the doucment.
   * @param document document to index.
   */
  @SneakyThrows
  public static void indexDocument(@NonNull EsClient esClient, String idxName, String type, long id, String document) {
    String endpoint = buildEndpoint(idxName, type, id);
    HttpEntity body = createBodyFromString(document);
    esClient.performPutRequest(endpoint, Collections.emptyMap(), body);
  }

  /**
   * Deletes a document from an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName index to remove the document from.
   * @param type type of the document.
   * @param id id of the document to be removed.
   */
  @SneakyThrows
  public static void deleteDocument(@NonNull EsClient esClient, String idxName, String type, long id) {
    String endpoint = buildEndpoint(idxName, type, id);
    esClient.performDeleteRequest(endpoint);
  }

  /**
   * Refreshes an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName index to be refreshed.
   */
  @SneakyThrows
  public static void refreshIndex(@NonNull EsClient esClient, String idxName) {
    String endpoint = buildEndpoint(idxName, "_refresh");
    esClient.performPostRequest(endpoint, Collections.emptyMap(), null);
  }

  /**
   * Deletes all the indexes of the ES instance.
   *
   * @param esClient client to call ES. It is required.
   */
  @SneakyThrows
  public static void deleteAllIndexes(@NonNull EsClient esClient) {
    esClient.performDeleteRequest("_all");
  }

  /**
   * Checks if an index exists in the ES instance.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName index to check.
   * @return true if the index exists, false otherwise.
   */
  public static boolean existsIndex(@NonNull EsClient esClient, String idxName) {

    String endpoint = buildEndpoint(idxName);
    try {
      esClient.performGetRequest(endpoint);
    } catch (ResponseException e) {
      if (HttpStatus.SC_NOT_FOUND == e.getResponse().getStatusLine().getStatusCode()) {
        return false;
      }
      throw new IllegalStateException("Error retreiving index", e);
    }
    return true;
  }

  /**
   * Deletes records in an index by some ES DSL query
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to delete records.
   * @param query ES DSL query
   */
  @SneakyThrows
  public static void deleteRecordsByQuery(@NonNull EsClient esClient, String idxName, String query) {
    String endpoint = buildEndpoint(idxName, "_delete_by_query?scroll_size=5000");
    HttpEntity body = createBodyFromString(query);
    esClient.performPostRequest(endpoint, Collections.emptyMap(), body);
  }

  static String buildEndpoint(Object... strings) {
    StringJoiner joiner = new StringJoiner("/");
    Arrays.stream(strings).forEach(x -> joiner.add(x.toString()));
    return "/" + joiner.toString();
  }
}
