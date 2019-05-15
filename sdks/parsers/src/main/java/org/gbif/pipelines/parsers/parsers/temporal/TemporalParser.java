package org.gbif.pipelines.parsers.parsers.temporal;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.gbif.pipelines.parsers.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.parsers.parsers.temporal.accumulator.ChronoAccumulatorConverter;
import org.gbif.pipelines.parsers.parsers.temporal.parser.ParserRawDateTime;
import org.gbif.pipelines.parsers.parsers.temporal.utils.DelimiterUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_INVALID;
import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_MISMATCH;
import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_UNLIKELY;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Main interpreter class. Interpreter for raw temporal period. The main method interpret two dates,
 * from and to
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalParser {

  private static final int ISSUE_SIZE = ParsedTemporalIssue.values().length;

  private static final BiFunction<ChronoAccumulator, Set<ParsedTemporalIssue>, Temporal> TEMPORAL_FUNC =
      (ca, deq) -> ChronoAccumulatorConverter.toTemporal(ca, deq).orElse(null);

  private static final Predicate<Temporal> HAS_DAY_FN = t -> t instanceof LocalDate || t instanceof LocalDateTime;
  private static final Predicate<Temporal> HAS_MONTH_FN = t -> HAS_DAY_FN.test(t) || t instanceof YearMonth;
  private static final Predicate<Temporal> HAS_YEAR_FN = t -> HAS_MONTH_FN.test(t) || t instanceof Year;

  private static final BiPredicate<Temporal, Year> HAS_YEAR_MATCH =
      (fromDate, year) -> year == null || Year.from(fromDate).equals(year);
  private static final BiPredicate<Temporal, Month> HAS_MONTH_MATCH =
      (fromDate, month) -> month == null || Month.from(fromDate).equals(month);

  public static ParsedTemporal parse(String rawDate) {
    return parse("", "", "", rawDate);
  }

  public static ParsedTemporal parse(String rawYear, String rawMonth, String rawDay, String rawDate) {
    // If year and rawDate are absent, return ParsedTemporalDates with NULL values inside
    if (isNullOrEmpty(rawYear) && isNullOrEmpty(rawDate)) {
      if (isNullOrEmpty(rawMonth) && isNullOrEmpty(rawDay)) {
        return ParsedTemporal.create();
      }
      return ParsedTemporal.create(DATE_INVALID);
    }

    // Parse year, month and day
    ParsedTemporal yearMonthDayParsed = parseYearMonthDayParsed(rawYear, rawMonth, rawDay);
    if (isNullOrEmpty(rawDate)) {
      return yearMonthDayParsed;
    }

    ParsedTemporal eventDateParsed = parseEventDate(rawDate);

    return mergeParsedTemporal(yearMonthDayParsed, eventDateParsed);
  }

  private static ParsedTemporal parseYearMonthDayParsed(String rawYear, String rawMonth, String rawDay) {
    Set<ParsedTemporalIssue> issues = new HashSet<>(ISSUE_SIZE);
    ChronoAccumulator accum = ChronoAccumulator.from(rawYear, rawMonth, rawDay);
    // Convert year, month and day parsed values
    Temporal temporal = TEMPORAL_FUNC.apply(accum, issues);
    Year year = HAS_YEAR_FN.test(temporal) ? Year.from(temporal) : null;
    Month month = HAS_MONTH_FN.test(temporal) ? Month.from(temporal) : null;
    Integer day = HAS_DAY_FN.test(temporal) ? MonthDay.from(temporal).getDayOfMonth() : null;

    boolean hasIssue = issues.contains(DATE_INVALID) || issues.contains(DATE_UNLIKELY);

    return hasIssue ? ParsedTemporal.create(issues) : ParsedTemporal.create(year, month, day, temporal, issues);
  }

  private static ParsedTemporal parseEventDate(String rawDate) {
    // Parse period
    String[] rawPeriod = DelimiterUtils.splitPeriod(rawDate);
    String rawFrom = rawPeriod[0];
    String rawTo = rawPeriod[1];

    ChronoAccumulator fromAccum = ParserRawDateTime.parse(rawFrom, null);

    ChronoField lastChronoField = fromAccum.getLastParsed().orElse(null);
    ChronoAccumulator toAccum = ParserRawDateTime.parse(rawTo, lastChronoField);

    // If toAccum doesn't contain last parsed value, raw date will consist of one date only
    if (toAccum.getLastParsed().isPresent()) {
      // Use toAccum value toAccum improve fromAccum parsed date
      toAccum.mergeAbsent(fromAccum);
    }

    Set<ParsedTemporalIssue> issues = new HashSet<>(ISSUE_SIZE);
    Temporal fromTemporal = TEMPORAL_FUNC.apply(fromAccum, issues);
    Temporal toTemporal = TEMPORAL_FUNC.apply(toAccum, issues);

    if (!isValidDateType(fromTemporal, toTemporal)) {
      toTemporal = null;
    }

    if (!isValidRange(fromTemporal, toTemporal)) {
      Temporal tmp = fromTemporal;
      fromTemporal = toTemporal;
      toTemporal = tmp;
    }

    return ParsedTemporal.create(fromTemporal, toTemporal, issues);
  }

  private static ParsedTemporal mergeParsedTemporal(ParsedTemporal yearMonthDayParsed, ParsedTemporal eventDateParsed) {

    if (!eventDateParsed.getFrom().isPresent() && !yearMonthDayParsed.getFrom().isPresent()) {
      return eventDateParsed.getIssueSet().isEmpty() ? yearMonthDayParsed : eventDateParsed;
    }

    if (!eventDateParsed.getFrom().isPresent()) {
      return yearMonthDayParsed;
    }

    if (eventDateParsed.getFrom().isPresent() && eventDateParsed.getTo().isPresent()) {
      return parseCaseFromAndTo(yearMonthDayParsed, eventDateParsed.getFrom().get(), eventDateParsed.getTo().get());
    }

    return parseCaseFrom(yearMonthDayParsed, eventDateParsed);
  }

  /** Update Year, month and day fields using parsed event date */
  private static ParsedTemporal parseCaseFrom(ParsedTemporal yearMonthDayParsed, ParsedTemporal eventDateParsed) {

    Temporal fromTemporal = eventDateParsed.getFrom().orElse(null);

    if (yearMonthDayParsed.getFrom().isPresent() && yearMonthDayParsed.getFrom().get().equals(fromTemporal)) {
      return yearMonthDayParsed;
    }

    if (fromTemporal == null) {
      return yearMonthDayParsed;
    }

    Year fromYear = HAS_YEAR_FN.test(fromTemporal) ? Year.from(fromTemporal) : null;
    Year year = yearMonthDayParsed.getYear().orElse(null);
    boolean isYearMatch = fromYear == null || year == null || fromYear.equals(year);
    Year resultYear = isYearMatch ? (fromYear == null ? year : fromYear) : null;

    Month fromMonth = HAS_MONTH_FN.test(fromTemporal) ? Month.from(fromTemporal) : null;
    Month month = yearMonthDayParsed.getMonth().orElse(null);
    boolean isMonthMatch = fromMonth == null || month == null || fromMonth.equals(month);
    Month resultMonth = isMonthMatch ? (fromMonth == null ? month : fromMonth) : null;

    Integer fromDay = HAS_DAY_FN.test(fromTemporal) ? MonthDay.from(fromTemporal).getDayOfMonth() : null;
    Integer day = yearMonthDayParsed.getDay().orElse(null);
    boolean isDayMatch = fromDay == null || day == null || fromDay.equals(day);
    Integer resultDay = isDayMatch ? (fromDay == null ? day : fromDay) : null;

    if (!isYearMatch || !isMonthMatch || !isDayMatch) {
      yearMonthDayParsed.setIssueSet(Collections.singleton(DATE_MISMATCH));
    }

    yearMonthDayParsed.setYear(resultYear);
    yearMonthDayParsed.setMonth(resultMonth);
    yearMonthDayParsed.setDay(resultDay);
    yearMonthDayParsed.setFromDate(resultYear, resultMonth, resultDay);

    return yearMonthDayParsed;
  }

  /** Update Year, month and day fields using parsed event date */
  private static ParsedTemporal parseCaseFromAndTo(ParsedTemporal yearMonthDayParsed, Temporal fromTemporal,
      Temporal toTemporal) {

    yearMonthDayParsed.setFromDate(fromTemporal);
    yearMonthDayParsed.setToDate(toTemporal);

    Year fromYear = Year.from(fromTemporal);
    if (!yearMonthDayParsed.getYear().isPresent()) {
      yearMonthDayParsed.setYear(fromYear);
    } else if (!yearMonthDayParsed.getYear().get().equals(fromYear)) {
      yearMonthDayParsed.setIssueSet(Collections.singleton(DATE_MISMATCH));
      return yearMonthDayParsed;
    }

    Optional<Year> year = yearMonthDayParsed.getYear();
    Month fromMonth = HAS_MONTH_FN.test(fromTemporal) ? Month.from(fromTemporal) : null;
    if (!yearMonthDayParsed.getMonth().isPresent() && year.isPresent()) {
      yearMonthDayParsed.setMonth(fromMonth);
    } else if (!yearMonthDayParsed.getMonth().get().equals(fromMonth)) {
      yearMonthDayParsed.setIssueSet(Collections.singleton(DATE_MISMATCH));
      return yearMonthDayParsed;
    }

    Optional<Month> month = yearMonthDayParsed.getMonth();
    Integer fromDay = HAS_DAY_FN.test(fromTemporal) ? MonthDay.from(fromTemporal).getDayOfMonth() : null;
    if (!yearMonthDayParsed.getDay().isPresent() && month.isPresent()) {
      yearMonthDayParsed.setDay(fromDay);
    } else if (yearMonthDayParsed.getDay().isPresent() && month.isPresent()
        && !yearMonthDayParsed.getDay().get().equals(fromDay)) {
      yearMonthDayParsed.setIssueSet(Collections.singleton(DATE_MISMATCH));
      return yearMonthDayParsed;
    }

    return yearMonthDayParsed;
  }

  /** Compare dates, FROM cannot be greater than TO */
  private static boolean isValidRange(Temporal from, Temporal to) {
    if (from == null || to == null) {
      return true;
    }
    TemporalUnit unit = null;
    if (from instanceof Year) {
      unit = ChronoUnit.YEARS;
    } else if (from instanceof YearMonth) {
      unit = ChronoUnit.MONTHS;
    } else if (from instanceof LocalDate) {
      unit = ChronoUnit.DAYS;
    } else if (from instanceof LocalDateTime) {
      unit = ChronoUnit.SECONDS;
    }
    return from.until(to, unit) >= 0;
  }

  /** Compare date types */
  private static boolean isValidDateType(Temporal from, Temporal to) {
    if (from == null) {
      return false;
    }
    if (to == null) {
      return true;
    }
    return from.getClass().equals(to.getClass());
  }

  /** Match dates base, example 2013-10 base equals 2013-10-11, example 2013-10 base equals null */
  private static boolean areDatesBaseMatch(ParsedTemporal yearMonthDayParsed, Temporal temporal) {
    Optional<Year> year = yearMonthDayParsed.getYear();
    if (year.isPresent() && !year.get().equals(Year.from(temporal))) {
      return false;
    }

    if (!HAS_MONTH_FN.test(temporal)) {
      return true;
    }
    Optional<Month> month = yearMonthDayParsed.getMonth();
    if (month.isPresent() && !month.get().equals(Month.from(temporal))) {
      return false;
    }

    if (!HAS_DAY_FN.test(temporal)) {
      return true;
    }
    Optional<Integer> day = yearMonthDayParsed.getDay();
    return day.isPresent() && day.get().equals(MonthDay.from(temporal).getDayOfMonth());
  }
}
