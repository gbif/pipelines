package org.gbif.pipelines.core.interpreter;

import org.gbif.pipelines.core.interpreter.temporal.ParsedTemporalDate;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TemporalInterpreterFunctionTest {

  private static final ZoneId ZONE_Z = ZoneId.of("Z");

  @Test
  public void Should_T_When_T_001() {
    //State
    String eventDate = null;

    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertTrue(result.hasIssue());
  }

  @Test
  public void Should_T_When_T_002() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 1, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = null;
    String year = "1999";
    String month = null;
    String day = null;

    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_003() {
    //State
    String eventDate = null;
    String year = null;
    String month = "04";
    String day = "01";

    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
    assertTrue(result.hasIssue());
  }

  @Test
  public void Should_T_When_T_004() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = null;
    String year = "1999";
    String month = "4";
    String day = "1";

    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_005() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_006() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_007() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_008() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 5, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04-05";
    String year = "1999";
    String month = "04";
    String day = "05";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_009() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 26, 0).atZone(ZoneId.of("Z"));

    String eventDate = "1999-04-01T09:26Z";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_010() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 33, 59).atZone(ZoneId.of("-0300"));

    String eventDate = "1999-04-01T09:33:59-0300";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_011() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 1, 1, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2010, 1, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999/2010";
    String year = "1999";
    String month = "01";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_012() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2010, 1, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04/2010-01";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_013() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(1999, 10, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04/10";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_014() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 12, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2009, 10, 8, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04-12/2009-10-08";
    String year = "1999";
    String month = "04";
    String day = "12";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_015() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 17, 12, 26, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(1999, 4, 17, 12, 52, 17).atZone(ZONE_Z);

    String eventDate = "1999-04-17T12:26Z/12:52:17Z";
    String year = "1999";
    String month = "04";
    String day = "17";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_016() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 8, 14, 7, 0).atZone(ZoneId.of("-0600"));
    ZonedDateTime expectedSecond = LocalDateTime.of(2010, 8, 3, 6, 0, 0).atZone(ZoneId.of("-0000"));

    String eventDate = "1999-04-08T14:07-0600/2010-08-03T06:00-0000";
    String year = "1999";
    String month = "04";
    String day = "08";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_017() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "01 Apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_018() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "01 apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_019() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "01 April 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_020() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "01-Apr-1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_021() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "ß1. Apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_022() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "apr-99";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_023() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "abr-99";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_024() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "April 01 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_025() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "April 01, 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_026() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "Apr. 1, 1999";
    String year = "1999";
    String month = "4";
    String day = "1";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_027() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "Apr. 01 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_028() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "01/04/1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_029() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999/04/01";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_030() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999/04/1";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_031() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999/4/1";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_032() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "April 01 1999";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_033() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "April 01, 1999";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_034() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "Apr. 01, 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_035() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "Apr. 01 1999";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_036() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 33, 59).atZone(ZoneId.of("-0300"));

    String eventDate = "1999-04-01T09:33:59-0300";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_037() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 1, 1, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2010, 1, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999/2010";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_038() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2010, 1, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04/2010-01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_039() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(1999, 10, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04/10";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_040() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 12, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2009, 10, 8, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04-12/2009-10-08";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_041() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 17, 12, 26, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(1999, 4, 17, 12, 52, 17).atZone(ZONE_Z);

    String eventDate = "1999-04-17T12:26Z/12:52:17Z";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_042() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 27, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2010, 1, 27, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04/2010-01";
    String year = "";
    String month = "";
    String day = "27";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_043() {
    //State
    boolean expectedHasIssue = true;

    String eventDate = "2100";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedHasIssue, result.hasIssue());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_044() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(2000, 12, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "12/2000";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_045() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(2000, 12, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "2000/12";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_046() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2010, 1, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04/2010-01";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_047() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1999, 4, 1, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(1999, 4, 11, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1999-04-01/11";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_048() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(2000, 2, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "2000/2";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }
  
  @Test
  public void Should_T_When_T_049() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(2016, 9, 15, 0, 5, 0).atZone(ZoneId.of("+1400"));

    String eventDate = "2016-09-15T00:05:00+1400 (LINT, Kiritimati, Kiribati - Christmas Island, UTC+14)";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_050() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(2009, 2, 13, 15, 20, 0).atZone(ZONE_Z);

    String eventDate = "2009-02-13 15:20";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_051() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1987, 4, 11, 9, 30, 0).atZone(ZONE_Z);

    String eventDate = "1987-04-11  9:30";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_052() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1958, 5, 5, 9, 0, 0).atZone(ZONE_Z);

    String eventDate = "1958-05-05 9:00";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_053() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1997, 12, 15, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "1997-12-15 00:00:00.0000000";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_054() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1, 1, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "NOTEBY J.Longino: St. 804, general collecting in canopy Basiloxylon, 30m high.";
    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_055() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(1, 1, 1, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = null;
    String year = "0";
    String month = null;
    String day = null;

    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void Should_T_When_T_056() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(2011, 9, 21, 0, 0, 0).atZone(ZONE_Z);
    ZonedDateTime expectedSecond = LocalDateTime.of(2011, 10, 5, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "2011-09-21/10-05";

    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
    assertEquals(expectedSecond, result.getTo().get().toZonedDateTime());
  }

  @Test
  public void Should_T_When_T_057() {
    //State
    ZonedDateTime expectedFirst = LocalDateTime.of(2012, 5, 6, 0, 0, 0).atZone(ZONE_Z);

    String eventDate = "20120506";

    //When
    ParsedTemporalDate result = TemporalInterpreterFunction.apply(eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get().toZonedDateTime());
  }

}