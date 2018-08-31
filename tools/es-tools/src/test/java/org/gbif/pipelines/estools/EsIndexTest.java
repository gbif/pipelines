package org.gbif.pipelines.estools;

import org.gbif.pipelines.estools.client.EsConfig;

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link EsIndex}. */
public class EsIndexTest {

  private static final String DUMMY_HOST = "http://dummy.com";

  /** {@link Rule} requires this field to be public. */
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test(expected = IllegalArgumentException.class)
  public void createIndexNullDatasetIdTest() {

    // When
    EsIndex.create(EsConfig.from(DUMMY_HOST), null, 1);

    // Should
    thrown.expectMessage("dataset id is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void createIndexEmptyDatasetIdTest() {

    // When
    EsIndex.create(EsConfig.from(DUMMY_HOST), "", 1);

    // Should
    thrown.expectMessage("dataset id is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasNullAliasTest() {

    // When
    EsIndex.swapIndexInAlias(EsConfig.from(DUMMY_HOST), null, "index_1");

    // Should
    thrown.expectMessage("alias is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasEmptyAliasTest() {

    // When
    EsIndex.swapIndexInAlias(EsConfig.from(DUMMY_HOST), "", "index_1");

    // Should
    thrown.expectMessage("alias is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasNullIndexTest() {

    // When
    EsIndex.swapIndexInAlias(EsConfig.from(DUMMY_HOST), "alias", null);

    // Should
    thrown.expectMessage("index is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasEmptyIndexTest() {

    // When
    EsIndex.swapIndexInAlias(EsConfig.from(DUMMY_HOST), "alias", "");

    // Should
    thrown.expectMessage("index is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasWrongFormatIndexTest() {

    // When
    EsIndex.swapIndexInAlias(EsConfig.from(DUMMY_HOST), "alias", "index");

    // Should
    thrown.expectMessage(CoreMatchers.containsString("index has to follow the pattern"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void countIndexDocumentsNullIndexTest() {

    // When
    EsIndex.countDocuments(EsConfig.from(DUMMY_HOST), null);

    // Should
    thrown.expectMessage("index is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void countIndexDocumentsEmptyIndexTest() {

    // When
    EsIndex.countDocuments(EsConfig.from(DUMMY_HOST), "");

    // Should
    thrown.expectMessage("index is required");
  }
}
