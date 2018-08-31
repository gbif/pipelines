package org.gbif.pipelines.estools.service;

import org.gbif.pipelines.estools.common.SettingsType;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
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

import static org.gbif.pipelines.estools.service.EsConstants.Action;
import static org.gbif.pipelines.estools.service.EsConstants.Constant;
import static org.gbif.pipelines.estools.service.EsConstants.Field;
import static org.gbif.pipelines.estools.service.EsConstants.Indexing;
import static org.gbif.pipelines.estools.service.EsConstants.Searching;
import static org.gbif.pipelines.estools.service.JsonHandler.createArrayNode;
import static org.gbif.pipelines.estools.service.JsonHandler.createObjectNode;
import static org.gbif.pipelines.estools.service.JsonHandler.writeToString;

/** Class that builds {@link HttpEntity} instances with JSON content. */
class HttpRequestBuilder {

  // pre-defined settings
  private static final ObjectNode INDEXING_SETTINGS = createObjectNode();
  private static final ObjectNode SEARCH_SETTINGS = createObjectNode();

  private JsonNode settings;
  private JsonNode mappings;
  private IndexAliasAction indexAliasAction;

  static {
    INDEXING_SETTINGS.put(Field.INDEX_REFRESH_INTERVAL, Indexing.REFRESH_INTERVAL);
    INDEXING_SETTINGS.put(Field.INDEX_NUMBER_SHARDS, Constant.NUMBER_SHARDS);
    INDEXING_SETTINGS.put(Field.INDEX_NUMBER_REPLICAS, Indexing.NUMBER_REPLICAS);
    INDEXING_SETTINGS.put(Field.INDEX_TRANSLOG_DURABILITY, Constant.TRANSLOG_DURABILITY);

    SEARCH_SETTINGS.put(Field.INDEX_REFRESH_INTERVAL, Searching.REFRESH_INTERVAL);
    SEARCH_SETTINGS.put(Field.INDEX_NUMBER_REPLICAS, Searching.NUMBER_REPLICAS);
  }

  private HttpRequestBuilder() {}

  /** Creates a new {@link HttpRequestBuilder}. */
  static HttpRequestBuilder newInstance() {
    return new HttpRequestBuilder();
  }

  /** Creates a {@link HttpEntity} from a {@link String} that will become the body of the entity. */
  static HttpEntity createBodyFromString(String body) {
    return createEntity(body);
  }

  /** Adds a {@link SettingsType} to the body. */
  HttpRequestBuilder withSettingsType(SettingsType settingsType) {
    Objects.requireNonNull(settingsType);
    this.settings = (settingsType == SettingsType.INDEXING) ? INDEXING_SETTINGS : SEARCH_SETTINGS;
    return this;
  }

  /** Adds a {@link Map} of settings to the body. */
  HttpRequestBuilder withSettingsMap(Map<String, String> settingsMap) {
    this.settings = JsonHandler.convertToJsonNode(settingsMap);
    return this;
  }

  /** Adds ES mappings in JSON format to the body. */
  HttpRequestBuilder withMappings(String mappings) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(mappings), "Mappings cannot be null or empty");
    this.mappings = JsonHandler.readTree(mappings);
    return this;
  }

  /** Adds ES mappings from a file in JSON format to the body. */
  HttpRequestBuilder withMappings(Path mappingsPath) {
    Objects.requireNonNull(mappingsPath, "The path of the mappings cannot be null");
    this.mappings = JsonHandler.readTree(loadFile(mappingsPath));
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
  HttpRequestBuilder withIndexAliasAction(
      String alias, Set<String> idxToAdd, Set<String> idxToRemove) {
    this.indexAliasAction = new IndexAliasAction(alias, idxToAdd, idxToRemove);
    return this;
  }

  HttpEntity build() {
    ObjectNode body = createObjectNode();

    // add settings
    if (settings != null) {
      body.set(Field.SETTINGS, settings);
    }

    // add mappings
    if (mappings != null) {
      body.set(Field.MAPPINGS, mappings);
    }

    // add alias actions
    if (indexAliasAction != null) {
      body.set(Field.ACTIONS, createIndexAliasActions(indexAliasAction));
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
    swapNode.put(Field.INDEX, idxToRemove);

    // add the node to the action
    ObjectNode action = createObjectNode();
    action.set(Action.REMOVE_INDEX, swapNode);
    actions.add(action);
  }

  private static void addIndexToAliasAction(String alias, String idx, ArrayNode actions) {
    // create swap node
    ObjectNode swapNode = createObjectNode();
    swapNode.put(Field.INDEX, idx);
    swapNode.put(Field.ALIAS, alias);

    // add the node to the action
    ObjectNode action = createObjectNode();
    action.set(Action.ADD, swapNode);
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

    final String alias;
    final Set<String> idxToAdd;
    final Set<String> idxToRemove;

    IndexAliasAction(String alias, Set<String> idxToAdd, Set<String> idxToRemove) {
      this.alias = alias;
      this.idxToAdd = idxToAdd;
      this.idxToRemove = idxToRemove;
    }
  }

  static InputStream loadFile(Path path) {
    if (path.isAbsolute()) {
      try {
        return new FileInputStream(path.toFile());
      } catch (FileNotFoundException exc) {
        throw new IllegalArgumentException(exc.getMessage(), exc);
      }
    } else {
      return Thread.currentThread().getContextClassLoader().getResourceAsStream(path.toString());
    }
  }
}
