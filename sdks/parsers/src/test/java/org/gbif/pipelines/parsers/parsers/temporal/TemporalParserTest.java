package org.gbif.pipelines.parsers.parsers.temporal;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.Temporal;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(JUnit4.class)
public class TemporalParserTest {

  @Test
  public void allNullTest() {
    // State
    String eventDate = null;
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void yearOnlyTest() {
    // State
    Temporal expectedFirst = Year.of(1999);

    String eventDate = null;
    String year = "1999";
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void yearAndEventDateNullTest() {
    // State
    String eventDate = null;
    String year = null;
    String month = "04";
    String day = "01";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void eventDateNullTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = null;
    String year = "1999";
    String month = "4";
    String day = "1";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void eventDateEmptyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void eventDateYearOnlyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void eventDateYearMonthOnlyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999-04";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateIsoTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 5);

    String eventDate = "1999-04-05";
    String year = "1999";
    String month = "04";
    String day = "05";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTimeIsoTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 26, 0);

    String eventDate = "1999-04-01T09:26Z";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void yearPeriodOnlyTest() {
    // State
    Temporal expectedFirst = Year.of(1999);
    Temporal expectedSecond = Year.of(2010);

    String eventDate = "1999/2010";
    String year = "1999";
    String month = "01";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void fullYearMonthPeriodTest() {
    // State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(2010, 1);

    String eventDate = "1999-04/2010-01";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void fullLocalDatePeriodTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 12);
    Temporal expectedSecond = LocalDate.of(2009, 10, 8);

    String eventDate = "1999-04-12/2009-10-08";
    String year = "1999";
    String month = "04";
    String day = "12";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void fullLocalDateTimePeriodTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 17, 12, 26, 0);
    Temporal expectedSecond = LocalDateTime.of(1999, 4, 17, 12, 52, 17);

    String eventDate = "1999-04-17T12:26Z/12:52:17Z";
    String year = "1999";
    String month = "04";
    String day = "17";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void fullLocalDateTimePeriodSkipZoneTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 8, 14, 7, 0);
    Temporal expectedSecond = LocalDateTime.of(2010, 8, 3, 6, 0, 0);

    String eventDate = "1999-04-08T14:07-0600/2010-08-03T06:00-0000";
    String year = "1999";
    String month = "04";
    String day = "08";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void localDateShortTextMonthTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 Apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateShortTextMistakeMonthTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateFullTextMonthTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 April 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateShortTextMonthDashTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01-Apr-1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateShortTextMonthDateMistakeTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "ß1. Apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void eventDateWrongYearTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "apr-99";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void eventDateWrongYearMonthMistakeTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "abr-99";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTextMonthFirstCommaTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "April 01, 1999";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateShortTextMonthFirstTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "Apr. 1, 1999";
    String year = "1999";
    String month = "4";
    String day = "1";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateShortTextMonthFirstDotTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "Apr. 01 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateSlashYearLastTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01/04/1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateSlashYearFirstTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/04/01";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateSlashShortDateTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/04/1";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateSlashShortMonthDateTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/4/1";
    String year = "1999";
    String month = "04";
    String day = "01";
    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTextMonthFirstTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "April 01 1999";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTimeSkipZoneTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 33, 59);

    String eventDate = "1999-04-01T09:33:59-0300";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void yearPeriodTest() {
    // State
    Temporal expectedFirst = Year.of(1999);
    Temporal expectedSecond = Year.of(2010);

    String eventDate = "1999/2010";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void shortYearMonthPeriodTest() {
    // State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(1999, 10);

    String eventDate = "1999-04/10";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void localDatePeriodTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 12);
    Temporal expectedSecond = LocalDate.of(2009, 10, 8);

    String eventDate = "1999-04-12/2009-10-08";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void localDateTimePeriodToTimeOnlyTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 17, 12, 26, 0);
    Temporal expectedSecond = LocalDateTime.of(1999, 4, 17, 12, 52, 17);

    String eventDate = "1999-04-17T12:26Z/12:52:17Z";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void featureYearTest() {
    String eventDate = "2100";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void yearMonthSlashMontFirstTest() {
    // State
    Temporal expectedFirst = YearMonth.of(2000, 12);

    String eventDate = "12/2000";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void yearMonthSlashTest() {
    // State
    Temporal expectedFirst = YearMonth.of(2000, 12);

    String eventDate = "2000/12";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void yearMonthPeriodTest() {
    // State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(2010, 1);

    String eventDate = "1999-04/2010-01";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void localDatePeriodToMonthOnlyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);
    Temporal expectedSecond = LocalDate.of(1999, 4, 11);

    String eventDate = "1999-04-01/11";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void yearMonthShortMonthSlashTest() {
    // State
    Temporal expectedFirst = YearMonth.of(2000, 2);

    String eventDate = "2000/2";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTimeSkipLongZoneTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(2016, 9, 15, 0, 5, 0);

    String eventDate =
        "2016-09-15T00:05:00+1400 (LINT, Kiritimati, Kiribati - Christmas Island, UTC+14)";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTimeSpaceTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(2009, 2, 13, 15, 20, 0);

    String eventDate = "2009-02-13 15:20";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTimeDoubleSpaceTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1987, 4, 11, 9, 30, 0);

    String eventDate = "1987-04-11  9:30";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTimeSpaceShortHourTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1958, 5, 5, 9, 0, 0);

    String eventDate = "1958-05-05 9:00";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDateTimeMillisecondsTest() {
    // State
    Temporal expectedFirst = LocalDateTime.of(1997, 12, 15, 0, 0, 0);

    String eventDate = "1997-12-15 00:00:00.0000000";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void textEventDateOnlyTest() {
    // State
    String eventDate =
        "NOTEBY J.Longino: St. 804, general collecting in canopy Basiloxylon, 30m high.";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void wrongYearOnlyTest() {
    // State
    String eventDate = null;
    String year = "0";
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);
    // Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void localDatePeriodToMonthDayTest() {
    // State
    Temporal expectedFirst = LocalDate.of(2011, 9, 21);
    Temporal expectedSecond = LocalDate.of(2011, 10, 5);

    String eventDate = "2011-09-21/10-05";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void localDateNumbersOnlyTest() {
    // State
    Temporal expectedFirst = LocalDate.of(2012, 5, 6);

    String eventDate = "20120506";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
  }

  @Test
  public void localDateTextEventDateTest() {
    // State
    Temporal expectedFirst = LocalDate.of(1999, 1, 1);

    String eventDate =
        "NOTEBY J.Longino: St. 804, general collecting in canopy Basiloxylon, 30m high.";
    String year = "1999";
    String month = "1";
    String day = "1";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void wrongLeapDayTest() {
    // State
    Temporal expectedFirst = YearMonth.of(2013, 2);

    String eventDate = "2013/2/29";
    String year = null;
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
  }

  @Test
  public void wrongLeapDayWithBaseTest() {
    // State
    Temporal expectedFirst = LocalDate.of(2013, 2, 28);

    String eventDate = "2013/2/29";
    String year = "2013";
    String month = "2";
    String day = "28";

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
  }

  @Test
  public void invalidPeriodTest() {

    // State
    Temporal expectedFirst = Year.of(2011);
    Temporal expectedSecond = Year.of(2013);

    String eventDate = "2013/2011";
    String year = "2013";
    String month = null;
    String day = null;

    // When
    ParsedTemporal result = TemporalParser.parse(year, month, day, eventDate);

    // Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }
}
