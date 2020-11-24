package org.gbif.pipelines.estools;

import java.util.Collections;
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
  public void swapIndexInAliasNullAliasTest() {

    // When
    EsIndex.swapIndexInAliases(EsConfig.from(DUMMY_HOST), null, "index_1");

    // Should
    thrown.expectMessage("aliases are required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasEmptyAliasTest() {
    // When
    EsIndex.swapIndexInAliases(EsConfig.from(DUMMY_HOST), Collections.singleton(""), "index_1");

    // Should
    thrown.expectMessage("aliases are required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasNullIndexTest() {

    // When
    EsIndex.swapIndexInAliases(EsConfig.from(DUMMY_HOST), Collections.singleton("alias"), null);

    // Should
    thrown.expectMessage("index is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasEmptyIndexTest() {

    // When
    EsIndex.swapIndexInAliases(EsConfig.from(DUMMY_HOST), Collections.singleton("alias"), "");

    // Should
    thrown.expectMessage("index is required");
  }

  @Test(expected = IllegalArgumentException.class)
  public void swapIndexInAliasWrongFormatIndexTest() {

    // When
    EsIndex.swapIndexInAliases(EsConfig.from(DUMMY_HOST), Collections.singleton("alias"), "index");

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
