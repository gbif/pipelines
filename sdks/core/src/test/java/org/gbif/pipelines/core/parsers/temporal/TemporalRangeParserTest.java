package org.gbif.pipelines.core.parsers.temporal;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY;
import static org.junit.Assert.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.temporal.TemporalAccessor;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.junit.Test;

public class TemporalRangeParserTest {

  @Test
  public void singleDateRangeTest() {
    TemporalRangeParser trp = TemporalRangeParser.builder().dateComponentOrdering(DMY).create();

    EventRange range = trp.parse("1930");
    assertResult(1930, 1, 1, 0, 0, 0, range.getFrom().get());
    assertResult(1930, 12, 31, 23, 59, 59, range.getTo().get());

    range = trp.parse("1930-01");
    assertResult(1930, 1, 1, 0, 0, 0, range.getFrom().get());
    assertResult(1930, 1, 31, 23, 59, 59, range.getTo().get());

    // Does not support
    range = trp.parse("01/1930");
    assertResult(1930, 1, 1, range.getFrom().get());
    assertResult(1930, 1, 31, range.getTo().get());
  }

  @Test
  public void rangeTest() {
    TemporalRangeParser trp = TemporalRangeParser.builder().dateComponentOrdering(DMY).create();

    EventRange range = trp.parse("1930-01-02/1930-02-01");
    assertResult(1930, 1, 2, 0, 0, 0, range.getFrom().get());
    assertResult(1930, 2, 1, 23, 59, 59, range.getTo().get());

    range = trp.parse("02/01/1930");
    assertResult(1930, 1, 2, range.getFrom().get());

    range = trp.parse("1930-01-02/02-01");
    assertResult(1930, 1, 2, range.getFrom().get());
    assertResult(1930, 2, 1, range.getTo().get());

    range = trp.parse("1930-01-02/15");
    assertResult(1930, 1, 2, range.getFrom().get());
    assertResult(1930, 1, 15, range.getTo().get());
  }

  @Test
  public void notRangeTest() {
    TemporalRangeParser trp = TemporalRangeParser.builder().dateComponentOrdering(DMY).create();

    EventRange range = trp.parse("1930-01-02");
    assertResult(1930, 1, 2, range.getFrom().get());
  }

  @Test
  public void ambigousDateTest() {
    // Use a default parser, which cannot understand like: 01/02/2020
    TemporalRangeParser trp = TemporalRangeParser.builder().create();

    EventRange parse = trp.parse("01/02/1999");
    assertTrue(parse.hasIssues());
    assertEquals(1, parse.getIssues().size());
    assertTrue(parse.getIssues().contains(OccurrenceIssue.RECORDED_DATE_INVALID));

    EventRange range = trp.parse("1999", "1", "2", "01/02/1999");
    assertResult(1999, 1, 2, range.getFrom().get());

    range = trp.parse("1999", "1", null, "01/02/1999");
    assertResult(1999, 1, range.getFrom().get());
    assertTrue(parse.hasIssues());
    assertEquals(1, parse.getIssues().size());
    assertTrue(parse.getIssues().contains(OccurrenceIssue.RECORDED_DATE_INVALID));
  }

  private void assertResult(int y, int m, TemporalAccessor result) {
    // sanity checks
    assertNotNull(result);

    YearMonth localDate = result.query(YearMonth::from);
    assertInts(y, localDate.getYear());
    assertInts(m, localDate.getMonthValue());
  }

  private void assertResult(int y, int m, int d, TemporalAccessor result) {
    // sanity checks
    assertNotNull(result);

    LocalDate localDate = result.query(LocalDate::from);
    assertInts(y, localDate.getYear());
    assertInts(m, localDate.getMonthValue());
    assertInts(d, localDate.getDayOfMonth());
  }

  private void assertResult(int y, int m, int d, int h, int mm, int s, TemporalAccessor result) {
    // sanity checks
    assertNotNull(result);

    LocalDateTime localDate = result.query(LocalDateTime::from);
    assertInts(y, localDate.getYear());
    assertInts(m, localDate.getMonthValue());
    assertInts(d, localDate.getDayOfMonth());
    assertInts(h, localDate.getHour());
    assertInts(mm, localDate.getMinute());
    assertInts(s, localDate.getSecond());
  }

  private void assertInts(Integer expected, Integer x) {
    if (expected == null) {
      assertNull(x);
    } else {
      assertEquals(expected, x);
    }
  }
}
