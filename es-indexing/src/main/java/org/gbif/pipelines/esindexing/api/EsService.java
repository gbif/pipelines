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

import static org.gbif.pipelines.esindexing.api.EndpointHelper.getAliasIndexexEndpoint;
import static org.gbif.pipelines.esindexing.api.EndpointHelper.getAliasesEndpoint;
import static org.gbif.pipelines.esindexing.api.EndpointHelper.getIndexEndpoint;
import static org.gbif.pipelines.esindexing.api.EndpointHelper.getIndexSettingsEndpoint;

/**
 * Service to perform ES operations.
 * <p>
 * The {@link EsClient} is always received as a parameter and this class has no responsibility on handling the
 * connection with the ES server.
 * <p>
 * <p>
 * This class is intended to be used internally within the same package, and <strong>never as a public API</strong>.
 * Therefore, the access modifiers should never be changed.
 */
class EsService {

  private static final Logger LOG = LoggerFactory.getLogger(EsService.class);

  private EsService() {}

  /**
   * Creates a ES index with the specified {@link SettingsType}.
   *
   * @param esClient     client to call ES. It is required.
   * @param idxName      name of the index to create.
   * @param settingsType settings to use in the call.
   *
   * @return name of the index created.
   */
  static String createIndexWithSettings(EsClient esClient, String idxName, SettingsType settingsType) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(idxName));

    // create entity body with settings
    HttpEntity entity = EntityBuilder.entityWithSettings(settingsType);

    try {
      Response response = esClient.performPutRequest(getIndexEndpoint(idxName), Collections.emptyMap(), entity);
      // parse response and return
      return ResponseParser.parseCreatedIndexResponse(response.getEntity());
    } catch (ResponseException exc) {
      LOG.error("Error when creating index {} with settings {}", idxName, settingsType, exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  /**
   * Updates the settings of an index.
   *
   * @param esClient     client to call ES. It is required.
   * @param idxName      name of the index to update.
   * @param settingsType settings that will be set to the index.
   */
  static void updateIndexSettings(EsClient esClient, String idxName, SettingsType settingsType) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(idxName));

    // create entity body with settings
    HttpEntity entity = EntityBuilder.entityWithSettings(settingsType);

    try {
      // perform the call
      esClient.performPutRequest(getIndexSettingsEndpoint(idxName), Collections.emptyMap(), entity);
    } catch (ResponseException exc) {
      LOG.error("Error when updating index {} to settings {}", idxName, settingsType, exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  /**
   * Gets all the indexes associated to a specific alias and whose names match with a specified pattern. Both alias
   * and the pattern are required.
   *
   * @param esClient   client to call ES. It is required.
   * @param idxPattern index to pattern. It can be the exact name of an index to do the query for a single index, or a
   *                   pattern using wildcards. For example, "idx*" matches with all the indexes whose name starts
   *                   with "idx".
   * @param alias      alias that has to be associated to the indexes retrieved.
   *
   * @return {@link Set} with all the indexes that are in the alias specified and match with the pattern received.
   */
  static Set<String> getIndexesByAliasAndIndexPattern(EsClient esClient, String idxPattern, String alias) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(idxPattern));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(alias));

    try {
      Response response = esClient.performGetRequest(getAliasIndexexEndpoint(idxPattern, alias));
      return ResponseParser.parseIndexesInAliasResponse(response.getEntity());
    } catch (ResponseException e) {
      LOG.debug("No indexes with pattern {} to remove from alias {}", idxPattern, alias);
      return Collections.emptySet();
    }
  }

  /**
   * Swaps indexes in an alias.
   * <p>
   * In this method we can add or remove indexes in an alias. Also note that in the case of removing indixes, they
   * are <strong>completely removed</strong> from the ES instance, and not only from the alias.
   *
   * @param esClient    client to call ES. It is required.
   * @param alias       alias that will be modified
   * @param idxToAdd    indexes to add to the alias.
   * @param idxToRemove indexes to remove from the alias.
   */
  static void swapIndexes(EsClient esClient, String alias, Set<String> idxToAdd, Set<String> idxToRemove) {
    Objects.requireNonNull(esClient);

    HttpEntity entity = EntityBuilder.entityIndexAliasActions(alias, idxToAdd, idxToRemove);
    try {
      esClient.performPostRequest(getAliasesEndpoint(), Collections.emptyMap(), entity);
    } catch (ResponseException exc) {
      LOG.error("Error when replacing index {} in alias {}", idxToAdd, alias, exc);
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

}
