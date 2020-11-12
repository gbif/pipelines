package org.gbif.pipelines.estools.service;

import static org.gbif.pipelines.estools.service.EsConstants.Action;
import static org.gbif.pipelines.estools.service.EsConstants.Field;
import static org.gbif.pipelines.estools.service.EsConstants.Indexing;
import static org.gbif.pipelines.estools.service.EsConstants.Searching;
import static org.gbif.pipelines.estools.service.JsonHandler.createArrayNode;
import static org.gbif.pipelines.estools.service.JsonHandler.createObjectNode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.gbif.pipelines.estools.common.SettingsType;
import org.gbif.pipelines.estools.model.IndexParams;

/** Class that builds {@link HttpEntity} instances with JSON content. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class HttpRequestBuilder {

  private JsonNode settings;
  private JsonNode mappings;
  private IndexAliasAction indexAliasAction;

  /** Creates a new {@link HttpRequestBuilder}. */
  static HttpRequestBuilder newInstance() {
    return new HttpRequestBuilder();
  }

  static HttpEntity createBodyFromIndexParams(IndexParams indexParams) {
    HttpRequestBuilder builder = new HttpRequestBuilder();

    if (indexParams.getMappings() != null) {
      builder.withMappings(indexParams.getMappings());
    }

    if (indexParams.getPathMappings() != null) {
      builder.withMappings(indexParams.getPathMappings());
    }

    if (indexParams.getSettingsType() != null) {
      builder.withSettingsType(indexParams.getSettingsType());
    }

    if (indexParams.getSettings() != null && !indexParams.getSettings().isEmpty()) {
      builder.withSettingsMap(indexParams.getSettings());
    }

    return builder.build();
  }

  /** Creates a {@link HttpEntity} from a {@link String} that will become the body of the entity. */
  static HttpEntity createBodyFromString(String body) {
    return createEntity(body);
  }

  /** Adds a {@link SettingsType} to the body. */
  HttpRequestBuilder withSettingsType(@NonNull SettingsType settingsType) {
    this.settings =
        (settingsType == SettingsType.INDEXING)
            ? JsonHandler.convertToJsonNode(Indexing.getDefaultIndexingSettings())
            : JsonHandler.convertToJsonNode(Searching.getDefaultSearchSettings());
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
  HttpRequestBuilder withMappings(@NonNull Path mappingsPath) {
    this.mappings = JsonHandler.readTree(loadFile(mappingsPath));
    return this;
  }

  /**
   * Adds actions to add and remove index from the aliases. Note that the indexes to be removed will
   * be removed completely from the ES instance.
   *
   * @param aliases aliases that wil be modify. This parameter is required.
   * @param idxToAdd indexes to add to the aliases.
   * @param idxToRemove indexes to remove. Note that these indexes will be completely removed form
   *     the ES instance.
   */
  HttpRequestBuilder withIndexAliasAction(
      Set<String> aliases, Set<String> idxToAdd, Set<String> idxToRemove) {
    this.indexAliasAction = new IndexAliasAction(aliases, idxToAdd, idxToRemove);
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
    Preconditions.checkArgument(
        indexAliasAction.aliases != null && !indexAliasAction.aliases.isEmpty());

    ArrayNode actions = createArrayNode();

    // remove all indices from alias action
    if (indexAliasAction.idxToRemove != null) {
      indexAliasAction.idxToRemove.forEach(idx -> removeIndexFromAliasAction(idx, actions));
    }
    // add index action
    if (indexAliasAction.idxToAdd != null) {
      indexAliasAction.idxToAdd.forEach(
          idx -> addIndexToAliasAction(indexAliasAction.aliases, idx, actions));
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

  private static void addIndexToAliasAction(Set<String> aliases, String idx, ArrayNode actions) {
    aliases.forEach(
        alias -> {
          // create swap node
          ObjectNode swapNode = createObjectNode();
          swapNode.put(Field.INDEX, idx);
          swapNode.put(Field.ALIAS, alias);

          // add the node to the action
          ObjectNode action = createObjectNode();
          action.set(Action.ADD, swapNode);
          actions.add(action);
        });
  }

  private static HttpEntity createEntity(ObjectNode entityNode) {
    return createEntity(entityNode.toString());
  }

  @SneakyThrows
  private static HttpEntity createEntity(String body) {
    return new NStringEntity(body);
  }

  private static class IndexAliasAction {

    final Set<String> aliases;
    final Set<String> idxToAdd;
    final Set<String> idxToRemove;

    IndexAliasAction(Set<String> aliases, Set<String> idxToAdd, Set<String> idxToRemove) {
      this.aliases = aliases;
      this.idxToAdd = idxToAdd;
      this.idxToRemove = idxToRemove;
    }
  }

  @SneakyThrows
  static InputStream loadFile(Path path) {
    if (path.isAbsolute()) {
      return new FileInputStream(path.toFile());
    } else {
      return Thread.currentThread().getContextClassLoader().getResourceAsStream(path.toString());
    }
  }
}
