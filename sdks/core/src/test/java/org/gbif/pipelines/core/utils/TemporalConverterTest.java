package org.gbif.pipelines.core.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.LocalDate;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.io.avro.EventDate;
import org.junit.Test;

public class TemporalConverterTest {

  @Test
  public void nullTest() {
    // State
    Integer year = null;
    Integer month = null;
    Integer day = null;

    // When
    Optional<TemporalAccessor> temporal = TemporalConverter.from(year, month, day);

    // Should
    assertFalse(temporal.isPresent());
  }

  @Test
  public void nullYearTest() {
    // State
    Integer year = null;
    Integer month = 10;
    Integer day = 1;

    // When
    Optional<TemporalAccessor> temporal = TemporalConverter.from(year, month, day);

    // Should
    assertFalse(temporal.isPresent());
  }

  @Test
  public void nullYearMonthTest() {
    // State
    Integer year = null;
    Integer month = null;
    Integer day = 1;

    // When
    Optional<TemporalAccessor> temporal = TemporalConverter.from(year, month, day);

    // Should
    assertFalse(temporal.isPresent());
  }

  @Test
  public void yearTest() {

    // State
    Integer year = 2000;
    Integer month = null;
    Integer day = null;

    // Expect
    Year expected = Year.of(year);

    // When
    Optional<TemporalAccessor> temporal = TemporalConverter.from(year, month, day);

    // Should
    assertTrue(temporal.isPresent());
    assertEquals(expected, temporal.get());
  }

  @Test
  public void yearMonthTest() {

    // State
    Integer year = 2000;
    Integer month = 10;
    Integer day = null;

    // Expect
    YearMonth expected = YearMonth.of(year, month);

    // When
    Optional<TemporalAccessor> temporal = TemporalConverter.from(year, month, day);

    // Should
    assertTrue(temporal.isPresent());
    assertEquals(expected, temporal.get());
  }

  @Test
  public void localDateTest() {

    // State
    Integer year = 2000;
    Integer month = 10;
    Integer day = 10;

    // Expect
    LocalDate expected = LocalDate.of(year, month, day);

    // When
    Optional<TemporalAccessor> temporal = TemporalConverter.from(year, month, day);

    // Should
    assertTrue(temporal.isPresent());
    assertEquals(expected, temporal.get());
  }

  @Test
  public void monthNullTest() {

    // State
    Integer year = 2000;
    Integer month = null;
    Integer day = 10;

    // Expect
    Year expected = Year.of(year);

    // When
    Optional<TemporalAccessor> temporal = TemporalConverter.from(year, month, day);

    // Should
    assertTrue(temporal.isPresent());
    assertEquals(expected, temporal.get());
  }

  @Test
  public void wrongDayMonthTest() {

    // State
    Integer year = 2000;
    Integer month = 11;
    Integer day = 31;
    // When
    Optional<TemporalAccessor> temporal = TemporalConverter.from(year, month, day);

    // Should
    assertFalse(temporal.isPresent());
  }

  @Test
  public void dateParserTest() {
    TemporalAccessor date = StringToDateFunctions.getStringToTemporalAccessor().apply("2019");
    assertEquals("2019", date.toString());

    date = StringToDateFunctions.getStringToTemporalAccessor().apply("2019-04");
    assertEquals("2019-04", date.toString());

    date = StringToDateFunctions.getStringToTemporalAccessor().apply("2019-04-02");
    assertEquals("2019-04-02", date.toString());

    date =
        StringToDateFunctions.getStringToTemporalAccessor().apply("2019-04-15T17:17:48.191 +02:00");
    assertEquals("2019-04-15T17:17:48.191+02:00", date.toString());

    date = StringToDateFunctions.getStringToTemporalAccessor().apply("2019-04-15T17:17:48.191");
    assertEquals("2019-04-15T17:17:48.191", date.toString());

    date =
        StringToDateFunctions.getStringToTemporalAccessor().apply("2019-04-15T17:17:48.023+02:00");
    assertEquals("2019-04-15T17:17:48.023+02:00", date.toString());

    date = StringToDateFunctions.getStringToTemporalAccessor().apply("2019-11-12T13:24:56.963591");
    assertEquals("2019-11-12T13:24:56.963591", date.toString());
  }

  @Test
  public void dateWithYearZeroTest() {
    TemporalAccessor date = StringToDateFunctions.getStringToTemporalAccessor().apply("0000");
    assertEquals("1970", date.toString());

    date = StringToDateFunctions.getStringToTemporalAccessor().apply("0000-01");
    assertEquals("1970-01", date.toString());

    date = StringToDateFunctions.getStringToTemporalAccessor().apply("0000-01-01");
    assertEquals("1970-01-01", date.toString());

    date = StringToDateFunctions.getStringToTemporalAccessor().apply("0000-01-01T00:00:01.100");
    assertEquals("1970-01-01T00:00:01.100", date.toString());

    date =
        StringToDateFunctions.getStringToTemporalAccessor().apply("0000-01-01T17:17:48.191 +02:00");
    assertEquals("1970-01-01T17:17:48.191+02:00", date.toString());

    date = StringToDateFunctions.getStringToTemporalAccessor().apply("0000-01-01T13:24:56.963591");
    assertEquals("1970-01-01T13:24:56.963591", date.toString());

    date =
        StringToDateFunctions.getStringToTemporalAccessor().apply("0000-01-01T17:17:48.023+02:00");
    assertEquals("1970-01-01T17:17:48.023+02:00", date.toString());
  }

  @Test
  public void before1582Test() {
    // Note dates before this date need special handling if java.util.Date is used.
    EventDate ed =
        EventDate.newBuilder().setGte("1400-01-01T00:00:00").setLte("2023-09-11T14:21:00").build();

    assertEquals(
        "1400-01-01T00:00:00/2023-09-11T14:21:00",
        TemporalConverter.getEventDateToStringFn().apply(ed));

    assertEquals("1400", StringToDateFunctions.getTemporalToStringFn().apply(Year.of(1400)));
  }
}
