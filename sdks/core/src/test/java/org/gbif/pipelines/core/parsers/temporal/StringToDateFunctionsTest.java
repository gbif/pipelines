package org.gbif.pipelines.core.parsers.temporal;

import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;

public class StringToDateFunctionsTest {

  @Test
  public void getStringToEarliestEpochSecondsTest() {
    String v = "2007-04-2";
    Function<String, Long> fn = StringToDateFunctions.getStringToEarliestEpochSeconds(false);
    Assert.assertNull(fn.apply(v));
  }

  @Test
  public void getStringToLatestEpochSecondsTest() {
    String v = "2007-04-2";
    Function<String, Long> fn = StringToDateFunctions.getStringToLatestEpochSeconds(false);
    Assert.assertNull(fn.apply(v));
  }
}
