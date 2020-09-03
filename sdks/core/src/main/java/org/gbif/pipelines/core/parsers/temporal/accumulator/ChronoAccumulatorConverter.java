package org.gbif.pipelines.core.parsers.temporal.accumulator;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.OFFSET_SECONDS;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;
import static org.gbif.pipelines.core.parsers.temporal.ParsedTemporalIssue.DATE_INVALID;
import static org.gbif.pipelines.core.parsers.temporal.ParsedTemporalIssue.DATE_UNLIKELY;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.parsers.temporal.ParsedTemporalIssue;
import org.gbif.pipelines.core.parsers.temporal.utils.ParsedUnitUtils;

/** The main function convert ChronoAccumulator to Temporal in appropriative way */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ChronoAccumulatorConverter {

  private static final Year OLD_YEAR = Year.of(1600);

  private static final Map<ChronoField, Function<String, Optional<Integer>>> FN_MAP =
      new EnumMap<>(ChronoField.class);

  static {
    FN_MAP.put(YEAR, ParsedUnitUtils::parseYear);
    FN_MAP.put(MONTH_OF_YEAR, ParsedUnitUtils::parseMonth);
    FN_MAP.put(DAY_OF_MONTH, ParsedUnitUtils::parseDay);
    FN_MAP.put(HOUR_OF_DAY, ParsedUnitUtils::parseHour);
    FN_MAP.put(MINUTE_OF_HOUR, ParsedUnitUtils::parseMinute);
    FN_MAP.put(SECOND_OF_MINUTE, ParsedUnitUtils::parseSecond);
  }

  /**
   * Converts ChronoAccumulator to Temporal
   *
   * @return some Temporal value: Year, YearMonth, LocalDate, LocalDateTime
   */
  public static Optional<Temporal> toTemporal(
      ChronoAccumulator accumulator, Set<ParsedTemporalIssue> issues) {

    // Check Year
    Optional<Year> optYear = getYear(accumulator, issues);
    if (!optYear.isPresent() || issues.contains(DATE_UNLIKELY) || issues.contains(DATE_INVALID)) {
      return Optional.empty();
    }
    Year year = optYear.get();

    // Check Month
    Optional<Month> optMonth = getMonth(accumulator, issues);
    if (!optMonth.isPresent() && issues.contains(DATE_INVALID)) {

      // Swap, maybe US format
      Optional<String> dayOfMonth = accumulator.getChronoFileValue(DAY_OF_MONTH);
      Optional<String> monthOfYear = accumulator.getChronoFileValue(MONTH_OF_YEAR);
      if (dayOfMonth.isPresent()
          && monthOfYear.isPresent()
          && ParsedUnitUtils.parseMonth(dayOfMonth.get()).isPresent()
          && ParsedUnitUtils.parseDay(monthOfYear.get()).isPresent()) {
        accumulator.setChronoField(MONTH_OF_YEAR, dayOfMonth.get());
        accumulator.setChronoField(DAY_OF_MONTH, monthOfYear.get());
        issues.remove(DATE_INVALID);

        optMonth = getMonth(accumulator, issues);
      } else {
        return Optional.empty();
      }

    } else if (!optMonth.isPresent()) {
      return Optional.of(year);
    }

    YearMonth yearMonth = year.atMonth(optMonth.get());
    // Check Day
    Optional<Integer> intDay = getDay(accumulator, issues);
    if (!intDay.isPresent()) {
      return Optional.of(yearMonth);
    }

    if (!yearMonth.isValidDay(intDay.get())) {
      issues.add(DATE_INVALID);
      return Optional.empty();
    }
    LocalDate localDate = yearMonth.atDay(intDay.get());

    // Check Hour
    Optional<Integer> intHour = convert(accumulator, HOUR_OF_DAY, issues);
    if (!intHour.isPresent()) {
      return Optional.of(localDate);
    }
    LocalDateTime localDateTime = localDate.atTime(intHour.get(), 0);

    // Check Minute
    Optional<Integer> intMinute = convert(accumulator, MINUTE_OF_HOUR, issues);
    if (!intMinute.isPresent()) {
      return Optional.of(localDateTime);
    }
    localDateTime = localDateTime.withMinute(intMinute.get());

    // Check Second
    Optional<Integer> intSecond = convert(accumulator, SECOND_OF_MINUTE, issues);
    if (!intSecond.isPresent()) {
      return Optional.of(localDateTime);
    }
    localDateTime = localDateTime.withSecond(intSecond.get());

    // Check time zone
    Optional<String> zone = accumulator.getChronoFileValue(OFFSET_SECONDS);
    if (!zone.isPresent()) {
      return Optional.of(localDateTime);
    }
    return Optional.of(localDateTime.atOffset(ZoneOffset.of(zone.get())));
  }

  /**
   * Looks for the year in a ChronoAccumulator
   *
   * @param accumulator - where to look for a value
   * @return Year value if present
   */
  public static Optional<Year> getYear(
      ChronoAccumulator accumulator, Set<ParsedTemporalIssue> issues) {
    Optional<Integer> integer = convert(accumulator, YEAR, issues);
    if (integer.isPresent()
        && (integer.get() < OLD_YEAR.getValue() || integer.get() > Year.now().getValue())) {
      issues.add(DATE_UNLIKELY);
      return Optional.empty();
    }
    return integer.map(Year::of);
  }

  /**
   * Looks for the month in a ChronoAccumulator
   *
   * @param accumulator - where to look for a value
   * @return Month value if present
   */
  public static Optional<Month> getMonth(
      ChronoAccumulator accumulator, Set<ParsedTemporalIssue> issues) {
    Optional<Integer> integer = convert(accumulator, MONTH_OF_YEAR, issues);
    if (integer.isPresent() && (integer.get() < 1 || integer.get() > 12)) {
      issues.add(DATE_INVALID);
      return Optional.empty();
    }
    return integer.map(Month::of);
  }

  /**
   * Looks for the day in a ChronoAccumulator
   *
   * @param accumulator - where to look for a value
   * @return Integer day value if present
   */
  public static Optional<Integer> getDay(
      ChronoAccumulator accumulator, Set<ParsedTemporalIssue> issues) {
    Optional<Integer> integer = convert(accumulator, DAY_OF_MONTH, issues);
    if (integer.isPresent() && (integer.get() < 1 || integer.get() > 31)) {
      issues.add(DATE_INVALID);
      return Optional.empty();
    }
    return integer;
  }

  /**
   * Converts raw value to integer and setChronoField into the map
   *
   * @param accumulator raw value for parsing
   * @param field one of the ChronoFields: YEAR, MONTH_OF_YEAR, DAY_OF_MONTH, HOUR_OF_DAY,
   *     MINUTE_OF_HOUR, SECOND_OF_MINUTE
   */
  private static Optional<Integer> convert(
      ChronoAccumulator accumulator, ChronoField field, Set<ParsedTemporalIssue> issues) {
    Optional<String> rawValue = accumulator.getChronoFileValue(field);
    if (rawValue.isPresent()) {
      Optional<Integer> value = FN_MAP.get(field).apply(rawValue.get());
      if (!value.isPresent()) {
        issues.add(DATE_INVALID);
      }
      return value;
    }
    return Optional.empty();
  }
}
