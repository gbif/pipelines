package org.gbif.pipelines.esindexing.response;

import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.common.JsonHandler;
import org.gbif.pipelines.esindexing.common.SettingsType;

import java.util.Set;

import org.apache.http.HttpEntity;

import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_FIELD;

/**
 * Parser for the ES responses encapsulated in a {@link HttpEntity}.
 */
public class ResponseParser {

  private ResponseParser() {}

  /**
   * Parses the response from an index creation request.
   * <p>
   * Specifically designed to use with a request similar to the one used in
   * {@link org.gbif.pipelines.esindexing.api.EsService#createIndexWithSettings(EsClient, String, SettingsType)}.
   *
   * @param entity {@link HttpEntity} from the response.
   *
   * @return the name of the index created.
   */
  public static String parseCreatedIndexResponse(HttpEntity entity) {
    return JsonHandler.readValue(entity).get(INDEX_FIELD);
  }

  /**
   * Parses the response from a request that gets the indexes in an alias.
   * <p>
   * Specifically designed to use with a request similar to the one used in
   * {@link org.gbif.pipelines.esindexing.api.EsService#getIndexesByAliasAndIndexPattern(EsClient, String, String)}.
   *
   * @param entity {@link HttpEntity} from the response.
   *
   * @return {@link Set} with the indexes of the alias.
   */
  public static Set<String> parseIndexesInAliasResponse(HttpEntity entity) {
    return JsonHandler.readValue(entity).keySet();
  }

}
