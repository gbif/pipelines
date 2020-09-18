package org.gbif.pipelines.core.parsers.temporal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.parsers.temporal.utils.DelimiterUtils;
import org.junit.Test;
import org.mortbay.log.Log;

@Slf4j
public class DelimiterUtilsTest {

  @Test
  public void canDo() {
    Log.info("====   CAN DO   ====");
    Arrays.stream(DelimiterUtils.splitPeriod("1999/2000")).forEach(x -> Log.info(x));
    Arrays.stream(DelimiterUtils.splitPeriod("1999-01/1999-12")).forEach(x -> Log.info(x));
    ;
    Arrays.stream(DelimiterUtils.splitPeriod("2004-12-30T00:00:00+0000/2005-03-13T24:00:00+0000"))
        .forEach(x -> Log.info(x));
    Arrays.stream(DelimiterUtils.splitPeriod("01/12/1997")).forEach(x -> Log.info(x));
    Arrays.stream(DelimiterUtils.splitPeriod("01/12/19")).forEach(x -> Log.info(x));
    Arrays.stream(DelimiterUtils.splitPeriod("/12/19")).forEach(x -> Log.info(x));
    Arrays.stream(DelimiterUtils.splitPeriod("12/19")).forEach(x -> Log.info(x));
    Log.info("=================");
  }

  @Test
  public void cannotDo() {
    Log.info("====   CANNOT DO   ====");
    Arrays.stream(DelimiterUtils.splitPeriod("01/12/1997 01/01/1998")).forEach(x -> Log.info(x));
    Arrays.stream(DelimiterUtils.splitPeriod("12/1997/01/1998")).forEach(x -> Log.info(x));
    Arrays.stream(DelimiterUtils.splitPeriod("1998-9-30/10-7")).forEach(x -> Log.info(x));
    Log.info("=================");
  }

  @Test
  public void testISORange() {
    // todo
    // assertArrayEquals(new String[] {"1998-9-30", "1998-10-7"},
    // DelimiterUtils.splitPeriod("1998-9-30 & 10-7"));
    // assertArrayEquals(new String[]{"1999-01-20",
    // "1999-01-31"},DelimiterUtils.splitISODateRange("19990120/31"));
    assertArrayEquals(
        new String[] {"1999-01-20", "1999-01-31"},
        DelimiterUtils.splitISODateRange("1999-01-20/31"));
    assertArrayEquals(
        new String[] {"1999-01", "1999-12"}, DelimiterUtils.splitISODateRange("1999-01/12"));
    assertArrayEquals(
        new String[] {"1999-1", "1999-2"}, DelimiterUtils.splitISODateRange("1999-1/2"));
    assertArrayEquals(
        new String[] {"1999-1-21", "1999-1-6"}, DelimiterUtils.splitISODateRange("1999-1-21/6"));
    assertArrayEquals(
        new String[] {"1999-1-02", "1999-1-6"}, DelimiterUtils.splitISODateRange("1999-1-02/6"));
    assertNull(DelimiterUtils.splitISODateRange("1999-01/31")); // Wrong month
    assertNull(DelimiterUtils.splitISODateRange("1999-01-20"));

    assertArrayEquals(
        new String[] {"1999-01-20", "1999-01-31"}, DelimiterUtils.splitPeriod("1999-01-20/31"));
    assertArrayEquals(new String[] {"1999-01-20", ""}, DelimiterUtils.splitPeriod("1999-01-20"));
    assertArrayEquals(
        new String[] {"19990120", "19990230"}, DelimiterUtils.splitPeriod("19990120/19990230"));
    assertArrayEquals(
        new String[] {"1999-01", "1999-02"}, DelimiterUtils.splitPeriod("1999-01/02"));
    assertArrayEquals(
        new String[] {"1998-9-30", "1998-10-7"}, DelimiterUtils.splitPeriod("1998-9-30/10-7"));
  }

  @Test
  public void todo() {}
}
