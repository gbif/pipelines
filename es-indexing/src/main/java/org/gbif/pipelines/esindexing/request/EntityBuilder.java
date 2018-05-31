package org.gbif.pipelines.esindexing.request;

import java.io.UnsupportedEncodingException;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;

import static org.gbif.pipelines.esindexing.utils.JsonUtils.createArrayNode;
import static org.gbif.pipelines.esindexing.utils.JsonUtils.createObjectNode;
import static org.gbif.pipelines.esindexing.utils.JsonUtils.writeJsonToString;

public class EntityBuilder {

  private static final ObjectNode indexingSettings = createObjectNode();
  private static final ObjectNode searchSettings = createObjectNode();

  static {
    indexingSettings.put("index.refresh_interval", "-1");
    indexingSettings.put("index.number_of_shards", "3");
    indexingSettings.put("index.number_of_replicas", "0");
    indexingSettings.put("index.translog.durability", "async");

    searchSettings.put("index.refresh_interval", "1s");
    searchSettings.put("index.number_of_replicas", "1");
  }

  private EntityBuilder() {}

  public static EntityBuilder newInstance() {
    return new EntityBuilder();
  }

  // TODO: add mappings

  public static HttpEntity entityWithSettings(SettingsType settingsType) {
    Objects.requireNonNull(settingsType);

    ObjectNode entity = createObjectNode();
    entity.set("settings", settingsType == SettingsType.INDEXING ? indexingSettings : searchSettings);

    return createEntity(entity);
  }

  public static HttpEntity entityReplaceIndexAlias(String alias, String idxToAdd, Set<String> idxToRemove) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(alias));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(idxToAdd));

    ObjectNode entity = createObjectNode();
    ArrayNode actions = createArrayNode();
    entity.set("actions", actions);

    // remove all indixes from alias action
    if (idxToRemove != null) {
      idxToRemove.forEach(idx -> removeIndexFromAliasAction(alias, idx, actions));
    }
    // add index action
    addIndexToAliasAction(alias, idxToAdd, actions);

    return createEntity(entity);
  }

  private static void removeIndexFromAliasAction(String alias, String idxToRemove, ArrayNode actions) {
    ObjectNode action = createObjectNode();
    ObjectNode swapNode = createObjectNode();
    swapNode.put("index", idxToRemove);
    action.set("remove_index", swapNode);
    actions.add(action);
  }

  private static void addIndexToAliasAction(String alias, String idx, ArrayNode actions) {
    ObjectNode action = createObjectNode();
    ObjectNode swapNode = createObjectNode();
    swapNode.put("index", idx);
    swapNode.put("alias", alias);
    action.set("add", swapNode);
    actions.add(action);
  }

  private static HttpEntity createEntity(ObjectNode entityNode) {
    try {
      String body = writeJsonToString(entityNode);
      return new NStringEntity(body);
    } catch (UnsupportedEncodingException exc) {
      throw new IllegalStateException(exc.getMessage(), exc);
    }
  }

  public enum SettingsType {
    INDEXING, SEARCH
  }

}
