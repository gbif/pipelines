package org.gbif.pipelines.core.parsers.temporal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class DelimiterUtilsTest {

  @Test
  public void testISORange() {
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
    assertArrayEquals(
        new String[] {"1999-01-20", "1999-01-20"}, DelimiterUtils.splitPeriod("1999-01-20"));
    assertArrayEquals(
        new String[] {"19990120", "19990230"}, DelimiterUtils.splitPeriod("19990120/19990230"));
    assertArrayEquals(
        new String[] {"1999-01", "1999-02"}, DelimiterUtils.splitPeriod("1999-01/02"));
    assertArrayEquals(
        new String[] {"1998-9-30", "1998-10-7"}, DelimiterUtils.splitPeriod("1998-9-30/10-7"));
    assertArrayEquals(new String[] {"", ""}, DelimiterUtils.splitPeriod(null));
  }
}
