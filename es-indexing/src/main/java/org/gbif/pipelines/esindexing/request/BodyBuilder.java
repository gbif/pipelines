package org.gbif.pipelines.esindexing.request;

import org.gbif.pipelines.esindexing.common.FileUtils;
import org.gbif.pipelines.esindexing.common.JsonHandler;
import org.gbif.pipelines.esindexing.common.SettingsType;

import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
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
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_NUMBER_REPLICAS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_NUMBER_SHARDS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_REFRESH_INTERVAL_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_TRANSLOG_DURABILITY_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.MAPPINGS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.NUMBER_SHARDS;
import static org.gbif.pipelines.esindexing.common.EsConstants.REMOVE_INDEX_ACTION;
import static org.gbif.pipelines.esindexing.common.EsConstants.SEARCHING_NUMBER_REPLICAS;
import static org.gbif.pipelines.esindexing.common.EsConstants.SEARCHING_REFRESH_INTERVAL;
import static org.gbif.pipelines.esindexing.common.EsConstants.SETTINGS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.TRANSLOG_DURABILITY;
import static org.gbif.pipelines.esindexing.common.JsonHandler.createArrayNode;
import static org.gbif.pipelines.esindexing.common.JsonHandler.createObjectNode;
import static org.gbif.pipelines.esindexing.common.JsonHandler.writeToString;

/** Class that builds {@link HttpEntity} instances with JSON content. */
public class BodyBuilder {

  // pre-defined settings
  private static final ObjectNode indexingSettings = createObjectNode();
  private static final ObjectNode searchSettings = createObjectNode();

  private JsonNode settings;
  private JsonNode mappings;
  private IndexAliasAction indexAliasAction;

  static {
    indexingSettings.put(INDEX_REFRESH_INTERVAL_FIELD, INDEXING_REFRESH_INTERVAL);
    indexingSettings.put(INDEX_NUMBER_SHARDS_FIELD, NUMBER_SHARDS);
    indexingSettings.put(INDEX_NUMBER_REPLICAS_FIELD, INDEXING_NUMBER_REPLICAS);
    indexingSettings.put(INDEX_TRANSLOG_DURABILITY_FIELD, TRANSLOG_DURABILITY);

    searchSettings.put(INDEX_REFRESH_INTERVAL_FIELD, SEARCHING_REFRESH_INTERVAL);
    searchSettings.put(INDEX_NUMBER_REPLICAS_FIELD, SEARCHING_NUMBER_REPLICAS);
  }

  private BodyBuilder() {}

  /** Creates a new {@link BodyBuilder}. */
  public static BodyBuilder newInstance() {
    return new BodyBuilder();
  }

  /** Creates a {@link HttpEntity} from a {@link String} that will become the body of the entity. */
  public static HttpEntity createBodyFromString(String body) {
    return createEntity(body);
  }

  /** Adds a {@link SettingsType} to the body. */
  public BodyBuilder withSettingsType(SettingsType settingsType) {
    Objects.requireNonNull(settingsType);
    this.settings = settingsType == SettingsType.INDEXING ? indexingSettings : searchSettings;
    return this;
  }

  /** Adds a {@link java.util.Map} of settings to the body. */
  public BodyBuilder withSettingsMap(Map<String, String> settingsMap) {
    this.settings = JsonHandler.convertToJsonNode(settingsMap);
    return this;
  }

  /** Adds ES mappings in JSON format to the body. */
  public BodyBuilder withMappings(String mappings) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(mappings), "Mappings cannot be null or empty");
    this.mappings = JsonHandler.readTree(mappings);
    return this;
  }

  /** Adds ES mappings from a file in JSON format to the body. */
  public BodyBuilder withMappings(Path mappingsPath) {
    Objects.requireNonNull(mappingsPath, "The path of the mappings cannot be null");
    this.mappings = JsonHandler.readTree(FileUtils.loadFile(mappingsPath));
    return this;
  }

  /**
   * Adds actions to add and remove index from an alias. Note that the indexes to be removed will be
   * removed completely from the ES instance.
   *
   * @param alias alias that wil be modify. This parameter is required.
   * @param idxToAdd indexes to add to the alias.
   * @param idxToRemove indexes to remove from the alias. These indexes will be completely removed
   *     form the ES instance.
   */
  public BodyBuilder withIndexAliasAction(
      String alias, Set<String> idxToAdd, Set<String> idxToRemove) {
    this.indexAliasAction = new IndexAliasAction(alias, idxToAdd, idxToRemove);
    return this;
  }

  public HttpEntity build() {
    ObjectNode body = createObjectNode();

    // add settings
    if (Objects.nonNull(settings)) {
      body.set(SETTINGS_FIELD, settings);
    }

    // add mappings
    if (Objects.nonNull(mappings)) {
      body.set(MAPPINGS_FIELD, mappings);
    }

    // add alias actions
    if (Objects.nonNull(indexAliasAction)) {
      body.set(ACTIONS_FIELD, createIndexAliasActions(indexAliasAction));
    }

    // create entity and return
    return createEntity(body);
  }

  /**
   * Builds a {@link ArrayNode} with the specified JSON content to add and remove indexes from an
   * alias.
   */
  private ArrayNode createIndexAliasActions(IndexAliasAction indexAliasAction) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(indexAliasAction.alias));

    ArrayNode actions = createArrayNode();

    // remove all indixes from alias action
    if (indexAliasAction.idxToRemove != null) {
      indexAliasAction.idxToRemove.forEach(idx -> removeIndexFromAliasAction(idx, actions));
    }
    // add index action
    if (indexAliasAction.idxToAdd != null) {
      indexAliasAction.idxToAdd.forEach(
          idx -> addIndexToAliasAction(indexAliasAction.alias, idx, actions));
    }

    return actions;
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
    return createEntity(writeToString(entityNode));
  }

  private static HttpEntity createEntity(String body) {
    try {
      return new NStringEntity(body);
    } catch (UnsupportedEncodingException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  private static class IndexAliasAction {

    String alias;
    Set<String> idxToAdd;
    Set<String> idxToRemove;

    IndexAliasAction(String alias, Set<String> idxToAdd, Set<String> idxToRemove) {
      this.alias = alias;
      this.idxToAdd = idxToAdd;
      this.idxToRemove = idxToRemove;
    }
  }
}
