package org.gbif.pipelines.esindexing.api;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.gbif.pipelines.esindexing.client.EsClient;
import org.gbif.pipelines.esindexing.common.SettingsType;
import org.gbif.pipelines.esindexing.request.BodyBuilder;
import org.gbif.pipelines.esindexing.response.ResponseParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.gbif.pipelines.esindexing.api.EndpointHelper.*;
import static org.gbif.pipelines.esindexing.request.BodyBuilder.createBodyFromString;

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
class EsService {

  private static final Logger LOG = LoggerFactory.getLogger(EsService.class);

  private EsService() {}

  /**
   * Creates a ES index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to create.
   * @param settingsType settings to use in the call.
   * @return name of the index created.
   */
  static String createIndex(EsClient esClient, String idxName, SettingsType settingsType) {
    Objects.requireNonNull(esClient);

    // create entity body
    HttpEntity body = BodyBuilder.newInstance().withSettingsType(settingsType).build();

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
  static String createIndex(
      EsClient esClient, String idxName, SettingsType settingsType, Path mappings) {
    Objects.requireNonNull(esClient);

    // create entity body
    HttpEntity body =
        BodyBuilder.newInstance().withSettingsType(settingsType).withMappings(mappings).build();

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
  static String createIndex(
      EsClient esClient, String idxName, SettingsType settingsType, String mappings) {
    Objects.requireNonNull(esClient);

    // create entity body
    HttpEntity body =
        BodyBuilder.newInstance().withSettingsType(settingsType).withMappings(mappings).build();

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
  static String createIndex(
      EsClient esClient, String idxName, Map<String, String> settings, Path mappings) {
    Objects.requireNonNull(esClient);

    // create entity body
    HttpEntity body =
        BodyBuilder.newInstance().withSettingsMap(settings).withMappings(mappings).build();

    return createIndexInternal(esClient, idxName, body);
  }

  private static String createIndexInternal(EsClient esClient, String idxName, HttpEntity body) {
    try {
      Response response =
          esClient.performPutRequest(getIndexEndpoint(idxName), Collections.emptyMap(), body);
      // parse response and return
      return ResponseParser.parseCreatedIndexResponse(response.getEntity());
    } catch (ResponseException exc) {
      LOG.error("Error creating index {} with body {}", idxName, body.toString(), exc);
      throw new IllegalStateException("Error creating index", exc);
    }
  }

  /**
   * Updates the settings of an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName name of the index to update.
   * @param settingsType settings that will be set to the index.
   */
  static void updateIndexSettings(EsClient esClient, String idxName, SettingsType settingsType) {
    Objects.requireNonNull(esClient);

    // create entity body with settings
    HttpEntity body = BodyBuilder.newInstance().withSettingsType(settingsType).build();

    try {
      // perform the call
      esClient.performPutRequest(getIndexSettingsEndpoint(idxName), Collections.emptyMap(), body);
    } catch (ResponseException exc) {
      LOG.error("Error updating index {} to settings {}", idxName, settingsType, exc);
      throw new IllegalStateException("Error updating index", exc);
    }
  }

  /**
   * Gets all the indexes associated to a specific alias and whose names match with a specified
   * pattern.
   *
   * @param esClient client to call ES. It is required.
   * @param idxPattern index to pattern. It can be the exact name of an index to do the query for a
   *     single index, or a pattern using wildcards. For example, "idx*" matches with all the
   *     indexes whose name starts with "idx".
   * @param alias alias that has to be associated to the indexes retrieved.
   * @return {@link Set} with all the indexes that are in the alias specified and match with the
   *     pattern received.
   */
  static Set<String> getIndexesByAliasAndIndexPattern(
      EsClient esClient, String idxPattern, String alias) {
    Objects.requireNonNull(esClient);

    try {
      Response response = esClient.performGetRequest(getAliasIndexexEndpoint(idxPattern, alias));
      return ResponseParser.parseIndexesInAliasResponse(response.getEntity());
    } catch (ResponseException e) {
      LOG.debug("No indexes with pattern {} found in alias {}", idxPattern, alias);
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
  static void swapIndexes(
      EsClient esClient, String alias, Set<String> idxToAdd, Set<String> idxToRemove) {
    Objects.requireNonNull(esClient);

    HttpEntity body =
        BodyBuilder.newInstance().withIndexAliasAction(alias, idxToAdd, idxToRemove).build();
    try {
      esClient.performPostRequest(getAliasesEndpoint(), Collections.emptyMap(), body);
    } catch (ResponseException exc) {
      LOG.error("Error swapping index {} in alias {}", idxToAdd, alias, exc);
      throw new IllegalStateException("Error swapping indexes", exc);
    }
  }

  /**
   * Counts the number of documents of an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idxName index to get the count from.
   * @return number of documents of the index.
   */
  static long countIndexDocuments(EsClient esClient, String idxName) {
    Objects.requireNonNull(esClient);

    try {
      Response response = esClient.performGetRequest(getIndexCountEndpoint(idxName));
      return ResponseParser.parseIndexCountResponse(response.getEntity());
    } catch (ResponseException exc) {
      LOG.error("Could not get count from index {}", idxName);
      throw new IllegalStateException("Could not get count from index", exc);
    }
  }

  /**
   * Indexes a document in an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idx index where the document has to be indexed to.
   * @param type type of the document.
   * @param id id of the doucment.
   * @param document document to index.
   */
  static void indexDocument(EsClient esClient, String idx, String type, long id, String document) {
    Objects.requireNonNull(esClient);

    final String endpoint = EndpointHelper.getIndexEndpoint(idx) + "/" + type + "/" + id;

    HttpEntity body = createBodyFromString(document);

    try {
      esClient.performPutRequest(endpoint, Collections.emptyMap(), body);
    } catch (IOException exc) {
      LOG.error(
          "Could not index document with id {} and body {} in index {}", id, body.toString(), idx);
      throw new IllegalStateException("Could not index document", exc);
    }
  }

  /**
   * Deletes a document from an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idx index to remove the document from.
   * @param type type of the document.
   * @param id id of the document to be removed.
   */
  static void deleteDocument(EsClient esClient, String idx, String type, long id) {
    Objects.requireNonNull(esClient);

    final String endpoint = EndpointHelper.getIndexEndpoint(idx) + "/" + type + "/" + id;
    try {
      esClient.performDeleteRequest(endpoint);
    } catch (IOException exc) {
      LOG.error("Could not delete document with id {} in index {}", id, idx);
      throw new IllegalStateException("Could not delete document", exc);
    }
  }

  /**
   * Refreshes an index.
   *
   * @param esClient client to call ES. It is required.
   * @param idx index to be refreshed.
   */
  static void refreshIndex(EsClient esClient, String idx) {
    Objects.requireNonNull(esClient);

    try {
      esClient.performPostRequest(
          EndpointHelper.getRefreshIndexEndpoint(idx), Collections.emptyMap(), null);
    } catch (IOException exc) {
      LOG.error("Could not refresh index {}", idx);
      throw new IllegalStateException("Could not refresh index", exc);
    }
  }

  /**
   * Deletes all the indexes of the ES instance.
   *
   * @param esClient client to call ES. It is required.
   */
  static void deleteAllIndexes(EsClient esClient) {
    Objects.requireNonNull(esClient);

    try {
      esClient.performDeleteRequest("_all");
    } catch (ResponseException exc) {
      LOG.error("Could not delete all indexes");
      throw new IllegalStateException("Could not delete all indexes", exc);
    }
  }

  /**
   * Checks if an index exists in the ES instance.
   *
   * @param esClient client to call ES. It is required.
   * @param idx index to check.
   * @return true if the index exists, false otherwise.
   */
  static boolean existsIndex(EsClient esClient, String idx) {
    Objects.requireNonNull(esClient);

    try {
      esClient.performGetRequest(EndpointHelper.getIndexEndpoint(idx));
    } catch (ResponseException e) {
      if (HttpStatus.SC_NOT_FOUND == e.getResponse().getStatusLine().getStatusCode()) {
        return false;
      }
      throw new IllegalStateException("Error retreiving index", e);
    }
    return true;
  }
}
