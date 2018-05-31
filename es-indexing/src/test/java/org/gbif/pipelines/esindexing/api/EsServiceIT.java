package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.EsIntegrationTest;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Response;
import org.junit.Assert;
import org.junit.Test;

import static org.gbif.pipelines.esindexing.common.EsConstants.DURABILITY_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEXING_NUMBER_REPLICAS;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEXING_REFRESH_INTERVAL;
import static org.gbif.pipelines.esindexing.common.EsConstants.INDEX_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.NUMBER_REPLICAS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.NUMBER_SHARDS;
import static org.gbif.pipelines.esindexing.common.EsConstants.NUMBER_SHARDS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.REFRESH_INTERVAL_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.SEARCHING_NUMBER_REPLICAS;
import static org.gbif.pipelines.esindexing.common.EsConstants.SEARCHING_REFRESH_INTERVAL;
import static org.gbif.pipelines.esindexing.common.EsConstants.SETTINGS_FIELD;
import static org.gbif.pipelines.esindexing.common.EsConstants.TRANSLOG_DURABILITY;
import static org.gbif.pipelines.esindexing.common.EsConstants.TRANSLOG_FIELD;
import static org.gbif.pipelines.esindexing.common.JsonUtils.readTreeFromEntity;
import static org.gbif.pipelines.esindexing.common.SettingsType.INDEXING;
import static org.gbif.pipelines.esindexing.common.SettingsType.SEARCH;

public class EsServiceIT extends EsIntegrationTest {

  // TODO: run IT only for some phases/profiles??

  // TODO: add some methods from here to public api?? like getIndex or parseSettings from response

  @Test
  public void createAndUpdateIndexWithSettingsTest() {
    String idx = EsService.createIndexWithSettings(getEsClient(), "idx-settings", INDEXING);

    Response response = null;
    try {
      response = getRestClient().performRequest(HttpGet.METHOD_NAME, "/" + idx);
      Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      Assert.fail("idx not created");
    }

    // check settings
    JsonNode indexSettings = readTreeFromEntity(response.getEntity()).path(idx).path(SETTINGS_FIELD).path(INDEX_FIELD);
    Assert.assertEquals(INDEXING_REFRESH_INTERVAL, indexSettings.path(REFRESH_INTERVAL_FIELD).asText());
    Assert.assertEquals(NUMBER_SHARDS, indexSettings.path(NUMBER_SHARDS_FIELD).asText());
    Assert.assertEquals(INDEXING_NUMBER_REPLICAS, indexSettings.path(NUMBER_REPLICAS_FIELD).asText());
    Assert.assertEquals(TRANSLOG_DURABILITY, indexSettings.path(TRANSLOG_FIELD).path(DURABILITY_FIELD).asText());

    EsService.updateIndexSettings(getEsClient(), idx, SEARCH);

    response = null;
    try {
      response = getRestClient().performRequest(HttpGet.METHOD_NAME, "/" + idx);
      Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    } catch (IOException e) {
      Assert.fail("idx not updated");
    }

    // check settings
    indexSettings = readTreeFromEntity(response.getEntity()).path(idx).path(SETTINGS_FIELD).path(INDEX_FIELD);
    Assert.assertEquals(SEARCHING_REFRESH_INTERVAL, indexSettings.path(REFRESH_INTERVAL_FIELD).asText());
    Assert.assertEquals(NUMBER_SHARDS, indexSettings.path(NUMBER_SHARDS_FIELD).asText());
    Assert.assertEquals(SEARCHING_NUMBER_REPLICAS, indexSettings.path(NUMBER_REPLICAS_FIELD).asText());
    Assert.assertEquals(TRANSLOG_DURABILITY, indexSettings.path(TRANSLOG_FIELD).path(DURABILITY_FIELD).asText());
  }

  @Test(expected = IllegalStateException.class)
  public void createWrongIndexTest() {
    EsService.createIndexWithSettings(getEsClient(), "UPPERCASE", INDEXING);
  }

  @Test
  public void getIndexesByAliasTest() {

  }

}
