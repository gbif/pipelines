package org.gbif.pipelines.esindexing.request;

import org.gbif.pipelines.esindexing.common.EsConstants;
import org.gbif.pipelines.esindexing.common.EsConstants.Action;
import org.gbif.pipelines.esindexing.common.EsConstants.Constant;
import org.gbif.pipelines.esindexing.common.EsConstants.Field;
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
    assertTrue(node.has(Field.SETTINGS));

    assertEquals(4, node.path(Field.SETTINGS).size());
    assertEquals(
        EsConstants.Indexing.REFRESH_INTERVAL,
        node.path(Field.SETTINGS).path(Field.INDEX_REFRESH_INTERVAL).asText());
    assertEquals(
        EsConstants.Indexing.NUMBER_REPLICAS,
        node.path(Field.SETTINGS).path(Field.INDEX_NUMBER_REPLICAS).asText());
    assertEquals(
        Constant.NUMBER_SHARDS, node.path(Field.SETTINGS).path(Field.INDEX_NUMBER_SHARDS).asText());
    assertEquals(
        Constant.TRANSLOG_DURABILITY,
        node.path(Field.SETTINGS).path(Field.INDEX_TRANSLOG_DURABILITY).asText());

    // search settings
    entity = BodyBuilder.newInstance().withSettingsType(SettingsType.SEARCH).build();
    node = readTree(entity);
    assertEquals(2, node.path(Field.SETTINGS).size());
    assertTrue(node.has(Field.SETTINGS));
    assertEquals(
        EsConstants.Searching.REFRESH_INTERVAL,
        node.path(Field.SETTINGS).path(Field.INDEX_REFRESH_INTERVAL).asText());
    assertEquals(
        EsConstants.Searching.NUMBER_REPLICAS,
        node.path(Field.SETTINGS).path(Field.INDEX_NUMBER_REPLICAS).asText());
  }

  @Test
  public void bodyWithSettingsMapTest() {
    // settings map
    Map<String, String> settings = new HashMap<>();
    settings.put(Field.INDEX_NUMBER_REPLICAS, "1");
    settings.put(Field.INDEX_NUMBER_SHARDS, "2");

    HttpEntity entity = BodyBuilder.newInstance().withSettingsMap(settings).build();

    // assert entity
    JsonNode node = readTree(entity);
    assertTrue(node.has(Field.SETTINGS));

    assertEquals(2, node.path(Field.SETTINGS).size());
    assertEquals("1", node.path(Field.SETTINGS).path(Field.INDEX_NUMBER_REPLICAS).asText());
    assertEquals("2", node.path(Field.SETTINGS).path(Field.INDEX_NUMBER_SHARDS).asText());
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
    assertTrue(node.has(Field.ACTIONS));

    assertEquals(4, node.path(Field.ACTIONS).size());

    JsonNode actions = node.path(Field.ACTIONS);

    // add actions
    List<JsonNode> addActions = actions.findValues(Action.ADD);
    assertEquals(2, addActions.size());

    Set<String> indexesAdded = new HashSet<>();
    addActions.forEach(jsonNode -> indexesAdded.add(jsonNode.get(Field.INDEX).asText()));
    assertTrue(indexesAdded.containsAll(idxToAdd));
    assertEquals(idxToAdd.size(), indexesAdded.size());
    assertEquals(alias, addActions.get(0).get(Field.ALIAS).asText());
    assertEquals(alias, addActions.get(1).get(Field.ALIAS).asText());

    // remove index actions
    List<JsonNode> removeActions = actions.findValues(Action.REMOVE_INDEX);
    assertEquals(2, removeActions.size());

    Set<String> indexesRemoved = new HashSet<>();
    removeActions.forEach(jsonNode -> indexesRemoved.add(jsonNode.get(Field.INDEX).asText()));
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
    assertTrue(node.has(Field.SETTINGS));
    assertTrue(node.has(Field.MAPPINGS));
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
    assertTrue(mappingsNode.has(Field.MAPPINGS));

    JsonNode mappings = mappingsNode.path(Field.MAPPINGS);
    assertTrue(mappings.has("doc"));
    assertTrue(mappings.path("doc").has("properties"));
    assertTrue(mappings.path("doc").path("properties").has("test"));
    assertEquals("text", mappings.path("doc").path("properties").path("test").get("type").asText());
  }
}
