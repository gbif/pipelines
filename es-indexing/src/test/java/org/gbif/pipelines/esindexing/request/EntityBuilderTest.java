package org.gbif.pipelines.esindexing.request;

import org.gbif.pipelines.esindexing.common.SettingsType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;
import org.junit.Test;

import static org.gbif.pipelines.esindexing.common.EsConstants.ACTIONS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.ADD_ACTION;
import static org.gbif.pipelines.esindexing.common.EsConstants.ALIAS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_DURABILITY_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_NUMBER_REPLICAS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_NUMBER_SHARDS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_REFRESH_INTERVAL_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.REMOVE_INDEX_ACTION;
import static org.gbif.pipelines.esindexing.common.EsConstants.SETTINGS_FIELD;
import static org.gbif.pipelines.esindexing.common.JsonHandler.readTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link EntityBuilder}.
 */
public class EntityBuilderTest {

  @Test
  public void entityWithSettingsTest() {
    // index settings
    HttpEntity entity = EntityBuilder.entityWithSettings(SettingsType.INDEXING);
    JsonNode node = readTree(entity);
    assertTrue(node.has(SETTINGS_FIELD));

    assertEquals(4, node.path(SETTINGS_FIELD).size());
    assertTrue(node.path(SETTINGS_FIELD).has(INDEX_REFRESH_INTERVAL_FIELD));
    assertTrue(node.path(SETTINGS_FIELD).has(INDEX_NUMBER_REPLICAS_FIELD));
    assertTrue(node.path(SETTINGS_FIELD).has(INDEX_NUMBER_SHARDS_FIELD));
    assertTrue(node.path(SETTINGS_FIELD).has(INDEX_DURABILITY_FIELD));

    // search settings
    entity = EntityBuilder.entityWithSettings(SettingsType.SEARCH);
    node = readTree(entity);
    assertEquals(2, node.path(SETTINGS_FIELD).size());
    assertTrue(node.has(SETTINGS_FIELD));
    assertTrue(node.path(SETTINGS_FIELD).has(INDEX_REFRESH_INTERVAL_FIELD));
    assertTrue(node.path(SETTINGS_FIELD).has(INDEX_NUMBER_REPLICAS_FIELD));
  }

  @Test
  public void entityIndexAliasActionsTest() {
    final String alias = "alias";
    Set<String> idxToAdd = new HashSet<>(Arrays.asList("add1", "add2"));
    Set<String> idxToRemove = new HashSet<>(Arrays.asList("remove1", "remove2"));

    HttpEntity entity = EntityBuilder.entityIndexAliasActions(alias, idxToAdd, idxToRemove);
    JsonNode node = readTree(entity);
    assertTrue(node.has(ACTIONS_FIELD));

    assertEquals(4, node.path(ACTIONS_FIELD).size());

    JsonNode actions = node.path(ACTIONS_FIELD);

    // add actions
    List<JsonNode> addActions = actions.findValues(ADD_ACTION);
    assertEquals(2, addActions.size());

    Set<String> indexesAdded = new HashSet<>();
    addActions.forEach(jsonNode -> indexesAdded.add(jsonNode.get(INDEX_FIELD).asText()));
    assertTrue(indexesAdded.containsAll(idxToAdd));
    assertEquals(idxToAdd.size(), indexesAdded.size());
    assertEquals(alias, addActions.get(0).get(ALIAS_FIELD).asText());
    assertEquals(alias, addActions.get(1).get(ALIAS_FIELD).asText());

    // remove index actions
    List<JsonNode> removeActions = actions.findValues(REMOVE_INDEX_ACTION);
    assertEquals(2, removeActions.size());

    Set<String> indexesRemoved = new HashSet<>();
    removeActions.forEach(jsonNode -> indexesRemoved.add(jsonNode.get(INDEX_FIELD).asText()));
    assertTrue(indexesRemoved.containsAll(idxToRemove));
    assertEquals(idxToRemove.size(), indexesRemoved.size());
  }

}
