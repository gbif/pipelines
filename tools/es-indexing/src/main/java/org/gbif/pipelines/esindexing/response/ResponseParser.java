package org.gbif.pipelines.esindexing.response;

import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.common.JsonHandler;
import org.gbif.pipelines.esindexing.common.SettingsType;

import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;

import static org.gbif.pipelines.esindexing.common.EsConstants.Field;

/** Parser for the ES responses encapsulated in a {@link HttpEntity}. */
public class ResponseParser {

  private ResponseParser() {}

  /**
   * Parses the response from an index creation request.
   *
   * <p>Specifically designed to use with a request similar to the one used in {@link
   * org.gbif.pipelines.esindexing.api.EsService#createIndex(EsClient, String, SettingsType)}.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return the name of the index created.
   */
  public static String parseCreatedIndexResponse(HttpEntity entity) {
    return JsonHandler.readValue(entity).get(Field.INDEX);
  }

  /**
   * Parses the response from a request that gets the indexes in an alias.
   *
   * <p>Specifically designed to use with a request similar to the one used in {@link
   * org.gbif.pipelines.esindexing.api.EsService#getIndexesByAliasAndIndexPattern(EsClient, String,
   * String)}.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return {@link Set} with the indexes of the alias.
   */
  public static Set<String> parseIndexesInAliasResponse(HttpEntity entity) {
    return JsonHandler.readValue(entity).keySet();
  }

  /**
   * Parses the response from a request that gets the number of documents of an index.
   *
   * @param entity {@link HttpEntity} from the response.
   * @return number of documents of the index.
   */
  public static long parseIndexCountResponse(HttpEntity entity) {
    JsonNode node = JsonHandler.readTree(entity);
    return node.has(Field.COUNT) ? node.path(Field.COUNT).asLong() : 0L;
  }
}
