package org.gbif.pipelines.interpretation.parsers.temporal.accumulator;

import org.gbif.pipelines.interpretation.parsers.temporal.utils.ParsedUnitUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * The main function convert ChronoAccumulator to Temporal in approrative way
 */
public class ChronoAccumulatorConverter {

  private static final Map<ChronoField, Function<String, Integer>> FUNCTION_MAP = new EnumMap<>(ChronoField.class);

  static {
    FUNCTION_MAP.put(YEAR, ParsedUnitUtils::parseYear);
    FUNCTION_MAP.put(MONTH_OF_YEAR, ParsedUnitUtils::parseMonth);
    FUNCTION_MAP.put(DAY_OF_MONTH, ParsedUnitUtils::parseDay);
    FUNCTION_MAP.put(HOUR_OF_DAY, ParsedUnitUtils::parseHour);
    FUNCTION_MAP.put(MINUTE_OF_HOUR, ParsedUnitUtils::parseMinute);
    FUNCTION_MAP.put(SECOND_OF_MINUTE, ParsedUnitUtils::parseSecond);
  }

  private ChronoAccumulatorConverter() {
    // Can't have an instance
  }

  /**
   * Converts ChronoAccumulator to Temporal
   *
   * @return some Temporal value: Year, YearMonth, LocalDate, LocalDateTime
   */
  public static Optional<Temporal> toTemporal(ChronoAccumulator accumulator) {
    Integer intYear = convert(accumulator, YEAR);
    if (intYear == null) {
      return Optional.empty();
    }
    Year year = Year.of(intYear);

    Integer intMonth = convert(accumulator, MONTH_OF_YEAR);
    if (intMonth == null) {
      return Optional.of(year);
    }
    YearMonth yearMonth = year.atMonth(intMonth);

    Integer intDay = convert(accumulator, DAY_OF_MONTH);
    if (intDay == null || !yearMonth.isValidDay(intDay)) {
      return Optional.of(yearMonth);
    }
    LocalDate localDate = yearMonth.atDay(intDay);

    Integer intHour = convert(accumulator, HOUR_OF_DAY);
    if (intHour == null) {
      return Optional.of(localDate);
    }
    LocalDateTime localDateTime = localDate.atTime(intHour, 0);

    Integer intMonute = convert(accumulator, MINUTE_OF_HOUR);
    if (intMonute == null) {
      return Optional.of(localDateTime);
    }
    localDateTime = localDateTime.withMinute(intMonute);

    Integer intSecond = convert(accumulator, SECOND_OF_MINUTE);
    if (intSecond == null) {
      return Optional.of(localDateTime);
    }
    return Optional.of(localDateTime.withSecond(intSecond));
  }

  /**
   * Looks for the year in a ChronoAccumulator
   *
   * @param accumulator - where to look for a value
   *
   * @return Year value if present
   */
  public static Optional<Year> getYear(ChronoAccumulator accumulator) {
    return Optional.ofNullable(convert(accumulator, YEAR)).map(Year::of);
  }

  /**
   * Looks for the month in a ChronoAccumulator
   *
   * @param accumulator - where to look for a value
   *
   * @return Month value if present
   */
  public static Optional<Month> getMonth(ChronoAccumulator accumulator) {
    return Optional.ofNullable(convert(accumulator, MONTH_OF_YEAR)).map(Month::of);
  }

  /**
   * Looks for the day in a ChronoAccumulator
   *
   * @param accumulator - where to look for a value
   *
   * @return Integer day value if present
   */
  public static Optional<Integer> getDay(ChronoAccumulator accumulator) {
    return Optional.ofNullable(convert(accumulator, DAY_OF_MONTH));
  }

  /**
   * Converts raw value to integer and put into the map
   *
   * @param accumulator raw value for parsing
   * @param chronoField one of the ChronoFields: YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY, MINUTE_OF_HOUR, SECOND_OF_MINUTE
   */
  private static Integer convert(ChronoAccumulator accumulator, ChronoField chronoField) {
    String rawValue = accumulator.valueMap.get(chronoField);
    if (isEmpty(rawValue)) {
      return null;
    }
    return FUNCTION_MAP.get(chronoField).apply(rawValue);
  }

}
