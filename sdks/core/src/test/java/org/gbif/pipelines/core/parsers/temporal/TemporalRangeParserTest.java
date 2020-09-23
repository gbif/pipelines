package org.gbif.pipelines.core.parsers.temporal;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY;
import static org.junit.Assert.*;

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
    assertEquals("1930", range.getFrom().get().toString());
    assertFalse(range.getTo().isPresent());

    range = trp.parse("1930/1931");
    assertEquals("1930", range.getFrom().get().toString());
    assertEquals("1931", range.getTo().get().toString());

    range = trp.parse("1930/1930");
    assertEquals("1930", range.getFrom().get().toString());
    assertFalse(range.getTo().isPresent());

    range = trp.parse("1930-01");
    assertEquals("1930-01", range.getFrom().get().toString());
    assertFalse(range.getTo().isPresent());

    // Does not support
    range = trp.parse("01/1930");
    assertFalse(range.getFrom().isPresent());
    assertFalse(range.getTo().isPresent());

    range = trp.parse("1930-01-02/1930-02-01");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertEquals("1930-02-01", range.getTo().get().toString());

    range = trp.parse("02/01/1930");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertFalse(range.getTo().isPresent());

    range = trp.parse("1930-01-02/02-01");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertEquals("1930-02-01", range.getTo().get().toString());

    range = trp.parse("1930-01-02/15");
    assertEquals("1930-01-02", range.getFrom().get().toString());
    assertEquals("1930-01-15", range.getTo().get().toString());
  }

  @Test
  public void ambigousDateTest() {
    // Use a default parser, which cannot understand like: 01/02/2020
    TemporalRangeParser trp = TemporalRangeParser.builder().create();

    EventRange range = trp.parse("01/02/1999");
    assertTrue(range.hasIssues());
    assertEquals(1, range.getIssues().size());
    assertTrue(range.getIssues().contains(OccurrenceIssue.RECORDED_DATE_INVALID));
  }

  // https://github.com/gbif/parsers/issues/6
  @Test
  public void alternativePayloadTest() {
    TemporalRangeParser trp = TemporalRangeParser.builder().create();

    EventRange range = trp.parse("1999", "1", "2", "01/02/1999");
    assertEquals("1999-01-02", range.getFrom().get().toString());
    assertFalse(range.getTo().isPresent());

    range = trp.parse("1999", "1", null, "01/02/1999");
    assertEquals("1999-01-02", range.getFrom().get().toString());
    assertFalse(range.getTo().isPresent());
  }
}
