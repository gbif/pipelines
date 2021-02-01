package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class TissueParserTest {

  @Test
  public void tissueEmptyTest() {
    // State
    String value = "";

    // When
    Optional<Boolean> result = TissueParser.hasTissue(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void tissueNullTest() {
    // State
    String value = null;

    // When
    Optional<Boolean> result = TissueParser.hasTissue(value);

    // Should
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void tissueFrozenTest() {
    // State
    String value = "frozen carcass";

    // When
    Optional<Boolean> result = TissueParser.hasTissue(value);

    // Should
    Assert.assertTrue(result.isPresent());
  }

  @Test
  public void tissueTissueTest() {
    // State
    String value = "tissue something";

    // When
    Optional<Boolean> result = TissueParser.hasTissue(value);

    // Should
    Assert.assertTrue(result.isPresent());
  }

  @Test
  public void tissuePlusTTest() {
    // State
    String value = "+tissue something";

    // When
    Optional<Boolean> result = TissueParser.hasTissue(value);

    // Should
    Assert.assertTrue(result.isPresent());
  }
}
