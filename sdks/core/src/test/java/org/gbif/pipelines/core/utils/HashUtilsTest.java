package org.gbif.pipelines.core.utils;

import org.junit.Assert;
import org.junit.Test;

public class HashUtilsTest {

  @Test
  public void sha1Test() {
    // State
    String value = "af91c6ca-da34-4e49-ace3-3b125dbeab3c";
    String expected = "3521a4e173f1c42a18d431d128720dc60e430a73";

    // When
    String result = HashUtils.getSha1(value);

    // Should
    Assert.assertEquals(expected, result);
  }

  @Test
  public void sha1TwoValueTest() {
    // State
    String value1 = "af91c6ca-da34-4e49-ace3-3b125dbeab3c";
    String value2 = "f033adff-4dc4-4d20-9da0-4ed24cf59b61";
    String expected = "74cf926f4871c8f98acf392b098e406ab82765b5";

    // When
    String result = HashUtils.getSha1(value1, value2);

    // Should
    Assert.assertEquals(expected, result);
  }
}
