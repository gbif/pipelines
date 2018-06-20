package org.gbif.pipelines.esindexing.api;

import org.gbif.pipelines.esindexing.client.EsConfig;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link EsHandler}. */
public class EsHandlerTest {

  private static final String DUMMY_HOST = "http://dummy.com";

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void createIndexNullDatasetIdTest() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("dataset id is required");
    EsHandler.createIndex(EsConfig.from(DUMMY_HOST), null, 1);
  }

  @Test
  public void createIndexEmptyDatasetIdTest() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("dataset id is required");
    EsHandler.createIndex(EsConfig.from(DUMMY_HOST), "", 1);
  }

  @Test
  public void swapIndexInAliasNullAliasTest() {
    // null alias
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("alias is required");
    EsHandler.swapIndexInAlias(EsConfig.from(DUMMY_HOST), null, "index_1");
  }

  @Test
  public void swapIndexInAliasEmptyAliasTest() {
    // null alias
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("alias is required");
    EsHandler.swapIndexInAlias(EsConfig.from(DUMMY_HOST), "", "index_1");
  }

  @Test
  public void swapIndexInAliasNullIndexTest() {
    // null alias
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("index is required");
    EsHandler.swapIndexInAlias(EsConfig.from(DUMMY_HOST), "alias", null);
  }

  @Test
  public void swapIndexInAliasEmptyIndexTest() {
    // null alias
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("index is required");
    EsHandler.swapIndexInAlias(EsConfig.from(DUMMY_HOST), "alias", "");
  }

  @Test
  public void swapIndexInAliasWrongFormatIndexTest() {
    // wrong format index
    thrown.expectMessage(CoreMatchers.containsString("index has to follow the pattern"));
    EsHandler.swapIndexInAlias(EsConfig.from(DUMMY_HOST), "alias", "index");
  }

  @Test
  public void countIndexDocumentsNullIndexTest() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("index is required");
    EsHandler.countIndexDocuments(EsConfig.from(DUMMY_HOST), null);
  }

  @Test
  public void countIndexDocumentsEmptyIndexTest() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("index is required");
    EsHandler.countIndexDocuments(EsConfig.from(DUMMY_HOST), "");
  }
}
