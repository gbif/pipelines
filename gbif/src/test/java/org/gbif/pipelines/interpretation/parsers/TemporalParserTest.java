package org.gbif.pipelines.interpretation.parsers;

import org.gbif.pipelines.interpretation.parsers.temporal.ParsedTemporalDates;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.Temporal;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TemporalParserTest {

  @Test
  public void testAllNull() {
    //State
    String eventDate = null;
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testYearOnly() {
    //State
    Temporal expectedFirst = Year.of(1999);

    String eventDate = null;
    String year = "1999";
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testYearAndEventDateNull() {
    //State
    String eventDate = null;
    String year = null;
    String month = "04";
    String day = "01";

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testEventDateNull() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = null;
    String year = "1999";
    String month = "4";
    String day = "1";

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testEventDateEmpty() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testEventDateYearOnly() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testEventDateYearMonthOnly() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999-04";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateIso() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 5);

    String eventDate = "1999-04-05";
    String year = "1999";
    String month = "04";
    String day = "05";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTimeIso() {
    //State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 26, 0);

    String eventDate = "1999-04-01T09:26Z";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testYearPeriodOnly() {
    //State
    Temporal expectedFirst = Year.of(1999);
    Temporal expectedSecond = Year.of(2010);

    String eventDate = "1999/2010";
    String year = "1999";
    String month = "01";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testFullYearMonthPeriod() {
    //State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(2010, 1);

    String eventDate = "1999-04/2010-01";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testFullLocalDatePeriod() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 12);
    Temporal expectedSecond = LocalDate.of(2009, 10, 8);

    String eventDate = "1999-04-12/2009-10-08";
    String year = "1999";
    String month = "04";
    String day = "12";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testFullLocalDateTimePeriod() {
    //State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 17, 12, 26, 0);
    Temporal expectedSecond = LocalDateTime.of(1999, 4, 17, 12, 52, 17);

    String eventDate = "1999-04-17T12:26Z/12:52:17Z";
    String year = "1999";
    String month = "04";
    String day = "17";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testFullLocalDateTimePeriodSkipZone() {
    //State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 8, 14, 7, 0);
    Temporal expectedSecond = LocalDateTime.of(2010, 8, 3, 6, 0, 0);

    String eventDate = "1999-04-08T14:07-0600/2010-08-03T06:00-0000";
    String year = "1999";
    String month = "04";
    String day = "08";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testLocalDateShortTextMonth() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 Apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateShortTextMistakeMonth() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateFullTextMonth() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01 April 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateShortTextMonthDash() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01-Apr-1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateShortTextMonthDateMistake() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "ß1. Apr. 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testEventDateWrongYear() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "apr-99";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testEventDateWrongYearMonthMistake() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "abr-99";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTextMonthFirstComma() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "April 01, 1999";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateShortTextMonthFirst() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "Apr. 1, 1999";
    String year = "1999";
    String month = "4";
    String day = "1";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateShortTextMonthFirstDot() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "Apr. 01 1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateSlashYearLast() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "01/04/1999";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateSlashYearFirst() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/04/01";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateSlashShortDate() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/04/1";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateSlashShortMonthDate() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "1999/4/1";
    String year = "1999";
    String month = "04";
    String day = "01";
    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTextMonthFirst() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);

    String eventDate = "April 01 1999";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTimeSkipZone() {
    //State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 1, 9, 33, 59);

    String eventDate = "1999-04-01T09:33:59-0300";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testYearPeriod() {
    //State
    Temporal expectedFirst = Year.of(1999);
    Temporal expectedSecond = Year.of(2010);

    String eventDate = "1999/2010";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);


    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testShortYearMonthPeriod() {
    //State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(1999, 10);

    String eventDate = "1999-04/10";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testLocalDatePeriod() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 12);
    Temporal expectedSecond = LocalDate.of(2009, 10, 8);

    String eventDate = "1999-04-12/2009-10-08";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testLocalDateTimePeriodToTimeOnly() {
    //State
    Temporal expectedFirst = LocalDateTime.of(1999, 4, 17, 12, 26, 0);
    Temporal expectedSecond = LocalDateTime.of(1999, 4, 17, 12, 52, 17);

    String eventDate = "1999-04-17T12:26Z/12:52:17Z";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testFeatureYear() {
    String eventDate = "2100";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testYearMonthSlashMontFirst() {
    //State
    Temporal expectedFirst = YearMonth.of(2000, 12);

    String eventDate = "12/2000";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testYearMonthSlash() {
    //State
    Temporal expectedFirst = YearMonth.of(2000, 12);

    String eventDate = "2000/12";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testYearMonthPeriod() {
    //State
    Temporal expectedFirst = YearMonth.of(1999, 4);
    Temporal expectedSecond = YearMonth.of(2010, 1);

    String eventDate = "1999-04/2010-01";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testLocalDatePeriodToMonthOnly() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 4, 1);
    Temporal expectedSecond = LocalDate.of(1999, 4, 11);

    String eventDate = "1999-04-01/11";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testYearMonthShortMonthSlash() {
    //State
    Temporal expectedFirst = YearMonth.of(2000, 2);

    String eventDate = "2000/2";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTimeSkipLongZone() {
    //State
    Temporal expectedFirst = LocalDateTime.of(2016, 9, 15, 0, 5, 0);

    String eventDate = "2016-09-15T00:05:00+1400 (LINT, Kiritimati, Kiribati - Christmas Island, UTC+14)";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTimeSpace() {
    //State
    Temporal expectedFirst = LocalDateTime.of(2009, 2, 13, 15, 20, 0);

    String eventDate = "2009-02-13 15:20";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTimeDoubleSpace() {
    //State
    Temporal expectedFirst = LocalDateTime.of(1987, 4, 11, 9, 30, 0);

    String eventDate = "1987-04-11  9:30";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTimeSpaceShortHour() {
    //State
    Temporal expectedFirst = LocalDateTime.of(1958, 5, 5, 9, 0, 0);

    String eventDate = "1958-05-05 9:00";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDateTimeMilliseconds() {
    //State
    Temporal expectedFirst = LocalDateTime.of(1997, 12, 15, 0, 0, 0);

    String eventDate = "1997-12-15 00:00:00.0000000";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testTextEventDateOnly() {
    //State
    String eventDate = "NOTEBY J.Longino: St. 804, general collecting in canopy Basiloxylon, 30m high.";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testWrongYearOnly() {
    //State
    String eventDate = null;
    String year = "0";
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);
    //Should
    assertFalse(result.getFrom().isPresent());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testLocalDatePeriodToMonthDay() {
    //State
    Temporal expectedFirst = LocalDate.of(2011, 9, 21);
    Temporal expectedSecond = LocalDate.of(2011, 10, 5);

    String eventDate = "2011-09-21/10-05";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertEquals(expectedSecond, result.getTo().get());
  }

  @Test
  public void testLocalDateNumbersOnly() {
    //State
    Temporal expectedFirst = LocalDate.of(2012, 5, 6);

    String eventDate = "20120506";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
  }

  @Test
  public void testLocalDateTextEventDate() {
    //State
    Temporal expectedFirst = LocalDate.of(1999, 1, 1);

    String eventDate = "NOTEBY J.Longino: St. 804, general collecting in canopy Basiloxylon, 30m high.";
    String year = "1999";
    String month = "1";
    String day = "1";

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
    assertFalse(result.getTo().isPresent());
  }

  @Test
  public void testWrongLeapDay() {
    //State
    Temporal expectedFirst = YearMonth.of(2013, 2);

    String eventDate = "2013/2/29";
    String year = null;
    String month = null;
    String day = null;

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
  }

  @Test
  public void testWrongLeapDayWithBase() {
    //State
    Temporal expectedFirst = LocalDate.of(2013, 2, 28);

    String eventDate = "2013/2/29";
    String year = "2013";
    String month = "2";
    String day = "28";

    //When
    ParsedTemporalDates result = TemporalParser.parse(year, month, day, eventDate);

    //Should
    assertEquals(expectedFirst, result.getFrom().get());
  }

}