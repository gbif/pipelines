package org.gbif.pipelines.esindexing.request;

import org.gbif.pipelines.esindexing.common.JsonUtils;
import org.gbif.pipelines.esindexing.common.SettingsType;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpEntity;
import org.junit.Assert;
import org.junit.Test;

import static org.gbif.pipelines.esindexing.common.EsConstants.SETTINGS_FIELD;

public class EntityBuilderTest {

  @Test
  public void entityWithSettingsTest() {
    // index settings
    HttpEntity entity = EntityBuilder.entityWithSettings(SettingsType.INDEXING);
    JsonNode node = JsonUtils.readTreeFromEntity(entity);
    Assert.assertTrue(node.has(SETTINGS_FIELD));

    // TODO: also check that the number of settings added match with the expected ones
//    Assert.assertTrue(node.path(SETTINGS_FIELD).has(REFRESH_INTERVAL_FIELD));
//    Assert.assertTrue(node.path(SETTINGS_FIELD).has(NUMBER_REPLICAS_FIELD));
//    Assert.assertTrue(node.path(SETTINGS_FIELD).has(NUMBER_SHARDS_FIELD));
//    Assert.assertTrue(node.path(SETTINGS_FIELD).path(TRANSLOG_FIELD).has(EsConstants.DURABILITY_FIELD));
//
//    // search settings
//    entity = EntityBuilder.entityWithSettings(SettingsType.SEARCH);
//    node = JsonUtils.readTreeFromEntity(entity);
//    Assert.assertTrue(node.has(SETTINGS_FIELD));
//    Assert.assertTrue(node.path(SETTINGS_FIELD).has(REFRESH_INTERVAL_FIELD));
//    Assert.assertTrue(node.path(SETTINGS_FIELD).has(NUMBER_REPLICAS_FIELD));
  }

}
