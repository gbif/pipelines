package org.gbif.pipelines.core.parsers.temporal;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY;
import static org.junit.Assert.*;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.junit.Test;

public class TemporalRangeParserTest {

  @Test
  public void singleDateRangeTest() {
    TemporalRangeParser trp = TemporalRangeParser.builder().dateComponentOrdering(DMY).create();

    EventRange range = trp.parse("1930");
    assertEquals("1930-01-01T00:00", range.getFrom().get().toString());
    assertEquals("1930-12-31T23:59:59", range.getTo().get().toString());

    range = trp.parse("1930-01");
    assertEquals("1930-01-01T00:00", range.getFrom().get().toString());
    assertEquals("1930-01-31T23:59:59", range.getTo().get().toString());

    // Does not support
    range = trp.parse("01/1930");
    assertEquals("1930-01-01", range.getFrom().get().toString());
    assertEquals("1930-01-31", range.getTo().get().toString());
  }

  @Test
  public void rangeTest() {
    TemporalRangeParser trp = TemporalRangeParser.builder().dateComponentOrdering(DMY).create();

    EventRange range = trp.parse("1930-01-02/1930-02-01");
    assertEquals("1930-01-02T00:00", range.getFrom().get().toString());
    assertEquals("1930-02-01T23:59:59", range.getTo().get().toString());

    range = trp.parse("02/01/1930");
    assertEquals("1930-01-02", range.getFrom().get().toString());

    range = trp.parse("1930-01-02/02-01");
    assertEquals("1930-01-02T00:00", range.getFrom().get().toString());
    assertEquals("1930-02-01T23:59:59", range.getTo().get().toString());

    range = trp.parse("1930-01-02/15");
    assertEquals("1930-01-02T00:00", range.getFrom().get().toString());
    assertEquals("1930-01-15T23:59:59", range.getTo().get().toString());
  }

  @Test
  public void notRangeTest() {
    TemporalRangeParser trp = TemporalRangeParser.builder().dateComponentOrdering(DMY).create();

    EventRange range = trp.parse("1930-01-02");
    assertEquals("1930-01-02", range.getFrom().get().toString());
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
    assertEquals("1999-01-02", range.getFrom().get().toString());

    range = trp.parse("1999", "1", null, "01/02/1999");
    assertEquals("1999-01", range.getFrom().get().toString());
    assertTrue(parse.hasIssues());
    assertEquals(1, parse.getIssues().size());
    assertTrue(parse.getIssues().contains(OccurrenceIssue.RECORDED_DATE_INVALID));
  }
}
