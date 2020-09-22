package org.gbif.pipelines.core.parsers.temporal;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.temporal.TemporalAccessor;
import org.gbif.common.parsers.date.CustomizedTextDateParser;
import org.gbif.common.parsers.date.DateParsers;
import org.junit.Before;
import org.junit.Test;

public class TemporalRangeParserTest {

  private TemporalRangeParser trp;

  @Before
  public void init() {
    trp =
        TemporalRangeParser.builder()
            .temporalParser(CustomizedTextDateParser.getInstance(DMY_FORMATS))
            .build();
  }

  @Test
  public void singleDateRangeTest() throws ParseException {
    TemporalAccessor[] range = trp.parse("1930");
    assertResult(1930, 1, 1, 0, 0, 0, range[0]);
    assertResult(1930, 12, 31, 23, 59, 59, range[1]);

    range = trp.parse("1930-01");
    assertResult(1930, 1, 1, 0, 0, 0, range[0]);
    assertResult(1930, 1, 31, 23, 59, 59, range[1]);

    // Does not support
    //    range =  trp.parse("01/1930");
    //    assertResult(1930,1,1, range[0]);
    //    assertResult(1930,1,31, range[1]);
  }

  @Test
  public void rangeTest() throws ParseException {
    TemporalAccessor[] range = trp.parse("1930-01-02/1930-02-01");
    assertResult(1930, 1, 2, 0, 0, 0, range[0]);
    assertResult(1930, 2, 1, 23, 59, 59, range[1]);

    range = trp.parse("02/01/1930");
    assertResult(1930, 1, 2, range[0]);

    range = trp.parse("1930-01-02/02-01");
    assertResult(1930, 1, 2, range[0]);
    assertResult(1930, 2, 1, range[1]);

    range = trp.parse("1930-01-02/15");
    assertResult(1930, 1, 2, range[0]);
    assertResult(1930, 1, 15, range[1]);
  }

  @Test
  public void notRangeTest() throws ParseException {
    TemporalAccessor[] range = trp.parse("1930-01-02");
    assertResult(1930, 1, 2, range[0]);
  }

  @Test
  public void ambigousDateTest() {
    // Use a default parser, which cannot understand like: 01/02/2020
    trp = TemporalRangeParser.builder().temporalParser(DateParsers.defaultTemporalParser()).build();
    try {
      TemporalAccessor[] range = trp.parse("01/02/1999");
    } catch (ParseException e) {
      assertEquals("Invalid", e.getMessage());
    }

    try {
      TemporalAccessor[] range = trp.parse("1999", "1", "2", "01/02/1999");
      assertResult(1999, 1, 2, range[0]);

      range = trp.parse("1999", "1", null, "01/02/1999");
      assertResult(1999, 1, range[0]);
    } catch (ParseException e) {
      assertEquals("Invalid", e.getMessage());
    }
  }

  private void assertResult(int y, int m, TemporalAccessor result) {
    // sanity checks
    assertNotNull(result);

    YearMonth localDate = result.query(YearMonth::from);
    assertInts(Integer.valueOf(y), Integer.valueOf(localDate.getYear()));
    assertInts(Integer.valueOf(m), Integer.valueOf(localDate.getMonthValue()));
  }

  private void assertResult(int y, int m, int d, TemporalAccessor result) {
    // sanity checks
    assertNotNull(result);

    LocalDate localDate = result.query(LocalDate::from);
    assertInts(Integer.valueOf(y), Integer.valueOf(localDate.getYear()));
    assertInts(Integer.valueOf(m), Integer.valueOf(localDate.getMonthValue()));
    assertInts(Integer.valueOf(d), Integer.valueOf(localDate.getDayOfMonth()));
  }

  private void assertResult(int y, int m, int d, int h, int mm, int s, TemporalAccessor result) {
    // sanity checks
    assertNotNull(result);

    LocalDateTime localDate = result.query(LocalDateTime::from);
    assertInts(Integer.valueOf(y), Integer.valueOf(localDate.getYear()));
    assertInts(Integer.valueOf(m), Integer.valueOf(localDate.getMonthValue()));
    assertInts(Integer.valueOf(d), Integer.valueOf(localDate.getDayOfMonth()));
    assertInts(Integer.valueOf(h), Integer.valueOf(localDate.getHour()));
    assertInts(Integer.valueOf(mm), Integer.valueOf(localDate.getMinute()));
    assertInts(Integer.valueOf(s), Integer.valueOf(localDate.getSecond()));
  }

  private void assertInts(Integer expected, Integer x) {
    if (expected == null) {
      assertNull(x);
    } else {
      assertEquals(expected, x);
    }
  }
}
