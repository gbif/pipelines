package org.gbif.pipelines.estools.service;

import org.gbif.pipelines.estools.client.EsClient;
import org.gbif.pipelines.estools.common.SettingsType;

import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;

import static org.gbif.pipelines.estools.service.EsConstants.Field;

/** Parser for the ES responses encapsulated in a {@link HttpEntity}. */
class HttpResponseParser {

  private HttpResponseParser() {}

  /**
   * Parses the response from an index creation request.
   *
   * <p>Specifically designed to use with a request similar to the one used in {@link
   * org.gbif.pipelines.estools.service.EsService#createIndex(EsClient, String, SettingsType)}.
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
   * org.gbif.pipelines.estools.service.EsService#getIndexesByAliasAndIndexPattern(EsClient,
   * String, String)}.
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
}
