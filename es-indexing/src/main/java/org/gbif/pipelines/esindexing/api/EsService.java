package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.common.SettingsType;
import org.gbif.pipelines.esindexing.request.EntityBuilder;
import org.gbif.pipelines.esindexing.response.ResponseParser;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EsService {

  private static final Logger LOG = LoggerFactory.getLogger(EsService.class);

  // endpoints
  private static final String ALIASES_ENDPOINT = "/_aliases";
  private static final String INDEXES_BY_ALIAS_ENDPOINT = "/%s/_alias/%s";
  private static final String INDEX_ENDPOINT_PATTERN = "/%s";
  private static final String INDEX_SETTINGS_ENDPOINT_PATTERN = "/%s/_settings";

  private EsService() {}

  static String createIndexWithSettings(EsClient esClient, String idxName, SettingsType settingsType) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(idxName));

    try {
      Response response =
        executeIndexOperationWithSettings(esClient, String.format(INDEX_ENDPOINT_PATTERN, idxName), settingsType);

      return ResponseParser.parseIndexName(response);
    } catch (ResponseException exc) {
      LOG.error("Error when creating index {} with settings {}", idxName, settingsType, exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  static void updateIndexSettings(EsClient esClient, String idxName, SettingsType settingsType) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(idxName));

    try {
      executeIndexOperationWithSettings(esClient,
                                        String.format(INDEX_SETTINGS_ENDPOINT_PATTERN, idxName),
                                        settingsType);
    } catch (ResponseException exc) {
      LOG.error("Error when updating index {} to settings {}", idxName, settingsType, exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  private static Response executeIndexOperationWithSettings(
    EsClient esClient, String endpoint, SettingsType settingsType
  ) throws ResponseException {
    // create request body
    HttpEntity entity = EntityBuilder.entityWithSettings(settingsType);
    // perform the call
    return esClient.performPutRequest(endpoint, Collections.emptyMap(), entity);
  }

  static Set<String> getIndexesByAlias(EsClient esClient, String index, String alias) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(alias));

    try {
      Response response = esClient.performGetRequest(String.format(INDEXES_BY_ALIAS_ENDPOINT, index, alias));
      return ResponseParser.parseIndexes(response);
    } catch (ResponseException e) {
      LOG.debug("No indexes with prefix {} to remove from alias {}", index, alias);
      return Collections.emptySet();
    }
  }

  static void swapIndexes(EsClient esClient, String idxToAdd, String alias, Set<String> idxToRemove) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(idxToAdd));

    HttpEntity entity = EntityBuilder.entityReplaceIndexAlias(alias, Collections.singleton(idxToAdd), idxToRemove);
    try {
      esClient.performPostRequest(ALIASES_ENDPOINT, Collections.emptyMap(), entity);
    } catch (ResponseException exc) {
      LOG.error("Error when replacing index {} in alias {}", idxToAdd, alias, exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

}
