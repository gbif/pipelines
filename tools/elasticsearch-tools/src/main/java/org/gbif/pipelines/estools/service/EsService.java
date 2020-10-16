package org.gbif.pipelines.estools.service;

import static org.gbif.pipelines.estools.service.HttpRequestBuilder.createBodyFromString;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.model.DeleteByQueryTask;
import org.gbif.pipelines.estools.model.IndexParams;

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
   * Creates an ES index.
   *
   * @param esClient client to call ES. It is required.
   * @param indexParams parameters of the index
   * @return name of the index
   */
  @SneakyThrows
  public static String createIndex(@NonNull EsClient esClient, IndexParams indexParams) {
    // create entity body
    HttpEntity body = HttpRequestBuilder.createBodyFromIndexParams(indexParams);

    String endpoint = buildEndpoint(indexParams.getIndexName());

    Response response = esClient.performPutRequest(endpoint, Collections.emptyMap(), body);

    // parse response and return
    return HttpResponseParser.parseCreatedIndexResponse(response.getEntity());
  }

  /**
   * Updates the settings of an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to update.
   * @param settings settings that will be set to the index.
   */
  @SneakyThrows
  public static void updateIndexSettings(
      @NonNull EsClient esClient, String idxName, Map<String, String> settings) {

    // create entity body with settings
    HttpEntity body = HttpRequestBuilder.newInstance().withSettingsMap(settings).build();

    String endpoint = buildEndpoint(idxName, "_settings");
    esClient.performPutRequest(endpoint, Collections.emptyMap(), body);
  }

  /**
   * Gets all the indexes associated to a specific alias and whose names match with a specified
   * pattern.
   *
   * @param esClient client to call ES. It is required.
   * @param idxPattern index to pattern. It can be the exact name of an index to do the query for a
   *     single index, or a pattern using wildcards. For example, "idx*" matches with all the
   *     indexes whose name starts with "idx".
   * @param aliases aliases that has to be associated to the indexes retrieved.
   * @return {@link Set} with all the indexes that are in the alias specified and match with the
   *     pattern received.
   */
  public static Set<String> getIndexesByAliasAndIndexPattern(
      @NonNull EsClient esClient, String idxPattern, Set<String> aliases) {
    return getIndexesByAliasAndIndexPattern(esClient, idxPattern, String.join(",", aliases));
  }

  /**
   * Gets all the indexes associated to a specific alias and whose names match with a specified
   * pattern.
   *
   * @param esClient client to call ES. It is required.
   * @param idxPattern index to pattern. It can be the exact name of an index to do the query for a
   *     single index, or a pattern using wildcards. For example, "idx*" matches with all the
   *     indexes whose name starts with "idx".
   * @param alias alias that has to be associated to the indexes retrieved. To pass more than one
   *     alias they have to be separated by commas. E.g.: alias1,alias2.
   * @return {@link Set} with all the indexes that are in the alias specified and match with the
   *     pattern received.
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
   * Swaps indexes in aliases.
   *
   * <p>In this method we can add or remove indexes in an alias. Also note that in the case of
   * removing indexes, they are <strong>completely removed</strong> from the ES instance, and not
   * only from the alias.
   *
   * @param esClient client to call ES. It is required.
   * @param aliases aliases that will be modified
   * @param idxToAdd indexes to add to the alias.
   * @param idxToRemove indexes to remove from the alias.
   */
  @SneakyThrows
  public static void swapIndexes(
      @NonNull EsClient esClient,
      Set<String> aliases,
      Set<String> idxToAdd,
      Set<String> idxToRemove) {
    if ((idxToAdd == null || idxToAdd.isEmpty())
        && (idxToRemove == null || idxToRemove.isEmpty())) {
      // nothing to swap
      return;
    }

    HttpEntity body =
        HttpRequestBuilder.newInstance()
            .withIndexAliasAction(aliases, idxToAdd, idxToRemove)
            .build();
    String aliasEndpoint = buildEndpoint("_aliases");

    if (idxToRemove.size() == 1) {
      String indexName = idxToRemove.iterator().next();
      String idxStatEndpoint = buildEndpoint(indexName, "_stats");
      long sleepTime = 300L;
      long attempts = 60_000L / sleepTime; // timeout 1 min
      while (attempts > 0L) {
        Response response = esClient.performGetRequest(idxStatEndpoint);
        long queryCurrent =
            JsonHandler.readTree(response.getEntity())
                .get("indices")
                .get(indexName)
                .get("primaries")
                .get("search")
                .get("query_current")
                .asLong();
        if (queryCurrent == 0) {
          break;
        }
        attempts--;
        TimeUnit.MILLISECONDS.sleep(sleepTime);
      }
    }
    esClient.performPostRequest(aliasEndpoint, Collections.emptyMap(), body);
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
   * @param id id of the document.
   * @param document document to index.
   */
  @SneakyThrows
  public static void indexDocument(
      @NonNull EsClient esClient, String idxName, String type, long id, String document) {
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
  public static void deleteDocument(
      @NonNull EsClient esClient, String idxName, String type, long id) {
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
   * Deletes records in an index by some ES DSL query and returns the ID of the task which is doing
   * the deletion.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to delete records.
   * @param query ES DSL query
   * @return the task ID
   */
  @SneakyThrows
  public static String deleteRecordsByQuery(
      @NonNull EsClient esClient, String idxName, String query) {
    String endpoint =
        buildEndpoint(
            idxName,
            "_delete_by_query?conflicts=proceed&scroll_size=5000&wait_for_completion=false");
    HttpEntity body = createBodyFromString(query);
    return HttpResponseParser.parseDeleteByQueryResponse(
        esClient.performPostRequest(endpoint, Collections.emptyMap(), body).getEntity());
  }

  @SneakyThrows
  public static DeleteByQueryTask getDeletedByQueryTask(@NonNull EsClient esClient, String taskId) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(taskId), "DeleteByQueryTask ID cannot be null or empty");

    return HttpResponseParser.parseDeleteByQueryTask(
        esClient.performGetRequest(buildEndpoint("_tasks", taskId)).getEntity());
  }

  /**
   * Finds the indexes in an alias where a given dataset is present.
   *
   * @param esClient client to call ES. It is required.
   * @param alias name of the alias to search in.
   */
  public static Set<String> findDatasetIndexesInAlias(
      @NonNull EsClient esClient, String alias, String datasetKey) {
    Response response =
        EsService.executeQuery(
            esClient, alias, String.format(EsQueries.FIND_DATASET_INDEXES_QUERY, datasetKey));
    return HttpResponseParser.parseFindDatasetIndexesInAliasResponse(response.getEntity());
  }

  /**
   * Executes a given DSL query.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to search in.
   * @param query ES DSL query.
   * @return {@link Response}
   */
  @SneakyThrows
  @VisibleForTesting
  public static Response executeQuery(@NonNull EsClient esClient, String idxName, String query) {
    String endpoint = buildEndpoint(idxName, "_search");
    HttpEntity body = createBodyFromString(query);
    return esClient.performPostRequest(endpoint, Collections.emptyMap(), body);
  }

  public static String buildEndpoint(Object... strings) {
    StringJoiner joiner = new StringJoiner("/");
    Arrays.stream(strings).forEach(x -> joiner.add(x.toString()));
    return "/" + joiner.toString();
  }
}
