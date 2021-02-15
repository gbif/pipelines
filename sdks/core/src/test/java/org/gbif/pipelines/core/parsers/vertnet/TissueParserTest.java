package org.gbif.pipelines.core.parsers.vertnet;

import org.junit.Assert;
import org.junit.Test;

public class TissueParserTest {

  @Test
  public void tissueEmptyTest() {
    // State
    String value = "";

    // When
    boolean result = TissueParser.hasTissue(value);

    // Should
    Assert.assertFalse(result);
  }

  @Test
  public void tissueNullTest() {
    // State
    String value = null;

    // When
    boolean result = TissueParser.hasTissue(value);

    // Should
    Assert.assertFalse(result);
  }

  @Test
  public void tissueFrozenTest() {
    // State
    String value = "frozen carcass";

    // When
    boolean result = TissueParser.hasTissue(value);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void tissueTissueTest() {
    // State
    String value = "tissue something";

    // When
    boolean result = TissueParser.hasTissue(value);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void tissuePlusTTest() {
    // State
    String value = "+tissue something";

    // When
    boolean result = TissueParser.hasTissue(value);

    // Should
    Assert.assertTrue(result);
  }
}
