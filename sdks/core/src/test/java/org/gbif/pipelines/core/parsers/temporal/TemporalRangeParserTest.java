package org.gbif.pipelines.core.parsers.temporal;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY;
import static org.gbif.common.parsers.date.DateComponentOrdering.DMYT;
import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.junit.Test;

public class TemporalRangeParserTest {

  @Test
  public void singleDateRangeTest() {
    TemporalRangeParser trp =
        TemporalRangeParser.builder()
            .temporalParser(TemporalParser.create(Collections.singletonList(DMY)))
            .create();

    EventRange range = trp.parse("1930/1929");
    assertEquals("1929", range.getFrom().get().toString());
    assertEquals("1930", range.getTo().get().toString());

    range = trp.parse("1930/1931");
    assertEquals("1930", range.getFrom().get().toString());
    assertEquals("1931", range.getTo().get().toString());

    range = trp.parse("1930/1930");
    assertEquals("1930", range.getFrom().get().toString());
    assertEquals("1930", range.getTo().get().toString());

    range = trp.parse("1930-01");
    assertEquals("1930-01", range.getFrom().get().toString());
    assertEquals("1930-01", range.getTo().get().toString());

    // Does not support
    range = trp.parse("01/1930");
    assertFalse(range.getFrom().isPresent());
    assertFalse(range.getTo().isPresent());

    range = trp.parse("1930-01-02/1930-02-01");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertEquals("1930-02-01", range.getTo().get().toString());

    range = trp.parse("02/01/1930");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertEquals("1930-01-02", range.getTo().get().toString());

    range = trp.parse("1930-01-02/02-01");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertEquals("1930-02-01", range.getTo().get().toString());

    range = trp.parse("1930-01-02/15");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertEquals("1930-01-15", range.getTo().get().toString());

    range = trp.parse("1930-01-02/1930-01-02");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertEquals("1930-01-02", range.getTo().get().toString());
  }

  @Test
  public void ambigousDateTest() {
    // Use a default parser, which cannot understand like: 01/02/2020
    TemporalRangeParser trp = TemporalRangeParser.builder().create();

    EventRange range = trp.parse("01/02/1999");
    assertTrue(range.hasIssues());
    assertEquals(1, range.getIssues().size());
    assertTrue(range.getIssues().contains(OccurrenceIssue.RECORDED_DATE_INVALID));

    range = trp.parse("1999-01-02");
    assertFalse(range.hasIssues());
    assertEquals("1999-01-02", range.getFrom().get().toString());
    assertEquals("1999-01-02", range.getTo().get().toString());

    // Use a DMY_FORMATS parser
    trp =
        TemporalRangeParser.builder()
            .temporalParser(TemporalParser.create(Arrays.asList(DMY_FORMATS)))
            .create();

    range = trp.parse("01/02/1999");
    assertFalse(range.hasIssues());
    assertEquals("1999-02-01", range.getFrom().get().toString());
    assertEquals("1999-02-01", range.getTo().get().toString());

    range = trp.parse("1999-01-02");
    assertFalse(range.hasIssues());
    assertEquals("1999-01-02", range.getFrom().get().toString());
    assertEquals("1999-01-02", range.getTo().get().toString());
  }

  // https://github.com/gbif/parsers/issues/6
  @Test
  public void alternativePayloadTest() {
    TemporalRangeParser trp = TemporalRangeParser.builder().create();

    EventRange range = trp.parse("1999", "1", "2", "01/02/1999");
    assertEquals("1999-01-02", range.getFrom().get().toString());
    assertEquals("1999-01-02", range.getTo().get().toString());

    range = trp.parse("1999", "1", null, "01/02/1999");
    assertEquals("1999-01", range.getFrom().get().toString());
    assertEquals("1999-01", range.getTo().get().toString());
  }

  @Test
  public void teatYMDT() {
    TemporalRangeParser trp =
        TemporalRangeParser.builder()
            .temporalParser(TemporalParser.create(Collections.singletonList(DMY)))
            .create();
    // Should fail
    EventRange range = trp.parse("01/03/1930T12:01");
    assertFalse(range.getFrom().isPresent());

    trp =
        TemporalRangeParser.builder()
            .temporalParser(TemporalParser.create(Arrays.asList(DMY, DMYT)))
            .create();
    range = trp.parse("01/03/1930T12:01");

    assertEquals("1930-03-01T12:01", range.getFrom().get().toString());
  }
}
