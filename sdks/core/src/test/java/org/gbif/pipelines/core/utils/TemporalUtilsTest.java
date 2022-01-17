package org.gbif.pipelines.core.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.LocalDate;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import org.junit.Test;

public class TemporalUtilsTest {

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
}
