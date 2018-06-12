package org.gbif.pipelines.esindexing.request;

import org.gbif.pipelines.esindexing.common.SettingsType;

import java.io.UnsupportedEncodingException;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;

import static org.gbif.pipelines.esindexing.common.EsConstants.ACTIONS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.ADD_ACTION;
import static org.gbif.pipelines.esindexing.common.EsConstants.ALIAS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEXING_NUMBER_REPLICAS;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEXING_REFRESH_INTERVAL;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_DURABILITY_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_NUMBER_REPLICAS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_NUMBER_SHARDS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_REFRESH_INTERVAL_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.NUMBER_SHARDS;
import static org.gbif.pipelines.esindexing.common.EsConstants.REMOVE_INDEX_ACTION;
import static org.gbif.pipelines.esindexing.common.EsConstants.SEARCHING_NUMBER_REPLICAS;
import static org.gbif.pipelines.esindexing.common.EsConstants.SEARCHING_REFRESH_INTERVAL;
import static org.gbif.pipelines.esindexing.common.EsConstants.SETTINGS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.TRANSLOG_DURABILITY;
import static org.gbif.pipelines.esindexing.common.JsonHandler.createArrayNode;
import static org.gbif.pipelines.esindexing.common.JsonHandler.createObjectNode;
import static org.gbif.pipelines.esindexing.common.JsonHandler.writeToString;

/**
 * Class that builds {@link HttpEntity} instances with JSON content.
 */
public class EntityBuilder {

  private static final ObjectNode indexingSettings = createObjectNode();
  private static final ObjectNode searchSettings = createObjectNode();

  static {
    indexingSettings.put(INDEX_REFRESH_INTERVAL_FIELD, INDEXING_REFRESH_INTERVAL);
    indexingSettings.put(INDEX_NUMBER_SHARDS_FIELD, NUMBER_SHARDS);
    indexingSettings.put(INDEX_NUMBER_REPLICAS_FIELD, INDEXING_NUMBER_REPLICAS);
    indexingSettings.put(INDEX_DURABILITY_FIELD, TRANSLOG_DURABILITY);

    searchSettings.put(INDEX_REFRESH_INTERVAL_FIELD, SEARCHING_REFRESH_INTERVAL);
    searchSettings.put(INDEX_NUMBER_REPLICAS_FIELD, SEARCHING_NUMBER_REPLICAS);
  }

  private EntityBuilder() {}

  // TODO: add mappings

  /**
   * Builds a {@link HttpEntity} with the specified ES {@link SettingsType} in the content as JSON.
   */
  public static HttpEntity entityWithSettings(SettingsType settingsType) {
    Objects.requireNonNull(settingsType);

    ObjectNode entity = createObjectNode();
    entity.set(SETTINGS_FIELD, settingsType == SettingsType.INDEXING ? indexingSettings : searchSettings);

    return createEntity(entity);
  }

  /**
   * Builds a {@link HttpEntity} with the specified JSON content to add and remove indexes from an alias.
   *
   * @param alias       alias
   * @param idxToAdd    indexes to add to the alias
   * @param idxToRemove indexes to remove from alias. Note that these indexes will be also removed from the ES instance.
   */
  public static HttpEntity entityIndexAliasActions(String alias, Set<String> idxToAdd, Set<String> idxToRemove) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(alias));

    ObjectNode entity = createObjectNode();
    ArrayNode actions = createArrayNode();
    entity.set(ACTIONS_FIELD, actions);

    // remove all indixes from alias action
    if (idxToRemove != null) {
      idxToRemove.forEach(idx -> removeIndexFromAliasAction(idx, actions));
    }
    // add index action
    if (idxToAdd != null) {
      idxToAdd.forEach(idx -> addIndexToAliasAction(alias, idx, actions));
    }

    return createEntity(entity);
  }

  private static void removeIndexFromAliasAction(String idxToRemove, ArrayNode actions) {
    // create swap node
    ObjectNode swapNode = createObjectNode();
    swapNode.put(INDEX_FIELD, idxToRemove);

    // add the node to the action
    ObjectNode action = createObjectNode();
    action.set(REMOVE_INDEX_ACTION, swapNode);
    actions.add(action);
  }

  private static void addIndexToAliasAction(String alias, String idx, ArrayNode actions) {
    // create swap node
    ObjectNode swapNode = createObjectNode();
    swapNode.put(INDEX_FIELD, idx);
    swapNode.put(ALIAS_FIELD, alias);

    // add the node to the action
    ObjectNode action = createObjectNode();
    action.set(ADD_ACTION, swapNode);
    actions.add(action);
  }

  private static HttpEntity createEntity(ObjectNode entityNode) {
    try {
      String body = writeToString(entityNode);
      return new NStringEntity(body);
    } catch (UnsupportedEncodingException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

}
