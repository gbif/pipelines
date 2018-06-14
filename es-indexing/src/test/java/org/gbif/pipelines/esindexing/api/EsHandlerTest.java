package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.client.EsConfig;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

public class EsHandlerTest {

  private static final String DUMMY_HOST = "http://dummy.com";

  @Test(expected = NullPointerException.class)
  public void createIndexNullConfigTest() {
    EsHandler.createIndex(null, "id", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createIndexIncompleteConfigTest() {
    EsHandler.createIndex(EsConfig.from(), "id", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createIndexNullDatasetIdTest() {
    EsHandler.createIndex(EsConfig.from(DUMMY_HOST), null, 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createIndexEmptyDatasetIdTest() {
    EsHandler.createIndex(EsConfig.from(DUMMY_HOST), "", 1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createIndexNullMappingsTest() {
    String mappings = null;
    EsHandler.createIndex(EsConfig.from(DUMMY_HOST), "id", 1, mappings);
  }

  @Test(expected = NullPointerException.class)
  public void createIndexNullMappingsPathTest() {
    Path mappings = null;
    EsHandler.createIndex(EsConfig.from(DUMMY_HOST), "id", 1, mappings);
  }

  @Test(expected = IllegalStateException.class)
  public void createIndexWrongMappingsPathTest() {
    Path mappings = Paths.get("fake-path");
    EsHandler.createIndex(EsConfig.from(DUMMY_HOST), "id", 1, mappings);
  }

  @Test(expected = NullPointerException.class)
  public void swapIndexInAliasNullConfigTest() {
    EsHandler.swapIndexInAlias(null, "alias", "index_1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasIncompleteConfigTest() {
    EsHandler.swapIndexInAlias(EsConfig.from(), "alias", "index_1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasNullAliasTest() {
    EsHandler.swapIndexInAlias(EsConfig.from(), null, "index_1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasEmptyAliasTest() {
    EsHandler.swapIndexInAlias(EsConfig.from(), "", "index_1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasNullIndexTest() {
    EsHandler.swapIndexInAlias(EsConfig.from(), "alias", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasEmptyIndexTest() {
    EsHandler.swapIndexInAlias(EsConfig.from(), "alias", "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasWrongIndexTest() {
    EsHandler.swapIndexInAlias(EsConfig.from(), "alias", "index");
  }

}
