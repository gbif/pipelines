package org.gbif.pipelines.esindexing.request;

import org.gbif.pipelines.esindexing.common.FileUtils;
import org.gbif.pipelines.esindexing.common.JsonHandler;
import org.gbif.pipelines.esindexing.common.SettingsType;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
import static org.gbif.pipelines.esindexing.common.JsonHandler.readTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the {@link BodyBuilder}. */
public class BodyBuilderTest {

  private static final String TEST_MAPPINGS_PATH = "mappings/simple-mapping.json";

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void bodyWithSettingsTest() {
    // index settings
    HttpEntity entity = BodyBuilder.newInstance().withSettingsType(SettingsType.INDEXING).build();

    // assert entity
    JsonNode node = readTree(entity);
    assertTrue(node.has(SETTINGS_FIELD));

    assertEquals(4, node.path(SETTINGS_FIELD).size());
    assertEquals(
        INDEXING_REFRESH_INTERVAL,
        node.path(SETTINGS_FIELD).path(INDEX_REFRESH_INTERVAL_FIELD).asText());
    assertEquals(
        INDEXING_NUMBER_REPLICAS,
        node.path(SETTINGS_FIELD).path(INDEX_NUMBER_REPLICAS_FIELD).asText());
    assertEquals(NUMBER_SHARDS, node.path(SETTINGS_FIELD).path(INDEX_NUMBER_SHARDS_FIELD).asText());
    assertEquals(
        TRANSLOG_DURABILITY,
        node.path(SETTINGS_FIELD).path(INDEX_TRANSLOG_DURABILITY_FIELD).asText());

    // search settings
    entity = BodyBuilder.newInstance().withSettingsType(SettingsType.SEARCH).build();
    node = readTree(entity);
    assertEquals(2, node.path(SETTINGS_FIELD).size());
    assertTrue(node.has(SETTINGS_FIELD));
    assertEquals(
        SEARCHING_REFRESH_INTERVAL,
        node.path(SETTINGS_FIELD).path(INDEX_REFRESH_INTERVAL_FIELD).asText());
    assertEquals(
        SEARCHING_NUMBER_REPLICAS,
        node.path(SETTINGS_FIELD).path(INDEX_NUMBER_REPLICAS_FIELD).asText());
  }

  @Test
  public void bodyWithSettingsMapTest() {
    // settings map
    Map<String, String> settings = new HashMap<>();
    settings.put(INDEX_NUMBER_REPLICAS_FIELD, "1");
    settings.put(INDEX_NUMBER_SHARDS_FIELD, "2");

    HttpEntity entity = BodyBuilder.newInstance().withSettingsMap(settings).build();

    // assert entity
    JsonNode node = readTree(entity);
    assertTrue(node.has(SETTINGS_FIELD));

    assertEquals(2, node.path(SETTINGS_FIELD).size());
    assertEquals("1", node.path(SETTINGS_FIELD).path(INDEX_NUMBER_REPLICAS_FIELD).asText());
    assertEquals("2", node.path(SETTINGS_FIELD).path(INDEX_NUMBER_SHARDS_FIELD).asText());
  }

  @Test
  public void bodyIndexAliasActionsTest() {
    final String alias = "alias";
    Set<String> idxToAdd = new HashSet<>(Arrays.asList("add1", "add2"));
    Set<String> idxToRemove = new HashSet<>(Arrays.asList("remove1", "remove2"));

    HttpEntity entity =
        BodyBuilder.newInstance().withIndexAliasAction(alias, idxToAdd, idxToRemove).build();

    // assert entity
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

  @Test
  public void bodyWithMappingsAsPath() {
    HttpEntity entity =
        BodyBuilder.newInstance().withMappings(Paths.get(TEST_MAPPINGS_PATH)).build();

    // assert entity
    JsonNode node = readTree(entity);
    assertMappings(node);
  }

  @Test
  public void bodyWithMappingsAsString() {
    String jsonMappings =
        JsonHandler.writeToString(FileUtils.loadFile(Paths.get(TEST_MAPPINGS_PATH)));

    HttpEntity entity = BodyBuilder.newInstance().withMappings(jsonMappings).build();

    // assert entity
    JsonNode node = readTree(entity);
    assertMappings(node);
  }

  @Test
  public void bodyWithSettingsAndMappings() {
    HttpEntity entity =
        BodyBuilder.newInstance()
            .withSettingsType(SettingsType.INDEXING)
            .withMappings(Paths.get(TEST_MAPPINGS_PATH))
            .build();

    // assert entity
    JsonNode node = readTree(entity);
    assertEquals(2, node.size());
    assertTrue(node.has(SETTINGS_FIELD));
    assertTrue(node.has(MAPPINGS_FIELD));
  }

  @Test
  public void bodyWithNullMappings() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Mappings cannot be null or empty");
    String mappings = null;
    BodyBuilder.newInstance().withMappings(mappings).build();
  }

  @Test
  public void bodyWithNullPathMappings() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("The path of the mappings cannot be null");
    Path mappings = null;
    BodyBuilder.newInstance().withMappings(mappings).build();
  }

  private void assertMappings(JsonNode mappingsNode) {
    assertTrue(mappingsNode.has(MAPPINGS_FIELD));

    JsonNode mappings = mappingsNode.path(MAPPINGS_FIELD);
    assertTrue(mappings.has("doc"));
    assertTrue(mappings.path("doc").has("properties"));
    assertTrue(mappings.path("doc").path("properties").has("test"));
    assertEquals("text", mappings.path("doc").path("properties").path("test").get("type").asText());
  }
}
