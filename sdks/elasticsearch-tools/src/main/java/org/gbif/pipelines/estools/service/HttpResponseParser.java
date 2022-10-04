package org.gbif.pipelines.estools.service;

import static org.gbif.pipelines.estools.service.EsConstants.Field;
import static org.gbif.pipelines.estools.service.EsQueries.AGG_BY_INDEX;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.http.HttpEntity;
import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.model.DeleteByQueryTask;

/** Parser for the ES responses encapsulated in a {@link HttpEntity}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class HttpResponseParser {

  /**
   * Parses the response from an index creation request.
   *
   * <p>Specifically designed to use with a request similar to the one used in {@link
   * org.gbif.pipelines.estools.service.EsService#createIndex(EsClient,
   * org.gbif.pipelines.estools.model.IndexParams)}.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return the name of the index created.
   */
  static String parseCreatedIndexResponse(HttpEntity entity) {
    return JsonHandler.readValue(entity).get(Field.INDEX);
  }

  /**
   * Parses the response from a request that gets the indexes in an alias.
   *
   * <p>Specifically designed to use with a request similar to the one used in {@link
   * org.gbif.pipelines.estools.service.EsService#getIndexesByAliasAndIndexPattern(EsClient, String,
   * String)}.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return {@link Set} with the indexes of the alias.
   */
  static Set<String> parseIndexesInAliasResponse(HttpEntity entity) {
    return JsonHandler.readValue(entity).keySet();
  }

  /**
   * Parses the response from a request that gets the number of documents of an index.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return number of documents of the index.
   */
  static long parseIndexCountResponse(HttpEntity entity) {
    JsonNode node = JsonHandler.readTree(entity);
    return node.has(Field.COUNT) ? node.path(Field.COUNT).asLong() : 0L;
  }

  /**
   * Parses the response from a request that finds the indexes where a dataset is indexed.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return indexes found
   */
  static Set<String> parseFindDatasetIndexesInAliasResponse(HttpEntity entity) {
    JsonNode node = JsonHandler.readTree(entity);
    return StreamSupport.stream(
            node.get("aggregations").get(AGG_BY_INDEX).get("buckets").spliterator(), false)
        .map(n -> n.get("key").asText())
        .collect(Collectors.toSet());
  }

  /**
   * Parses the response of delete by query and returns the task ID.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return task ID.
   */
  static String parseDeleteByQueryResponse(HttpEntity entity) {
    return JsonHandler.readTree(entity).get("task").asText();
  }

  /**
   * Parses the response of getting the delete by query task.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return {@link DeleteByQueryTask}
   */
  static DeleteByQueryTask parseDeleteByQueryTask(HttpEntity entity) {
    JsonNode node = JsonHandler.readTree(entity);
    DeleteByQueryTask task = new DeleteByQueryTask();
    task.setCompleted(node.get("completed").asBoolean());
    task.setRecordsDeleted(node.get("task").get("status").get("deleted").asLong());
    return task;
  }
}
