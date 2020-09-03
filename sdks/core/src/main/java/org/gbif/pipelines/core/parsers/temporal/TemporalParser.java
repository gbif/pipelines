package org.gbif.pipelines.core.parsers.temporal;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.gbif.pipelines.core.parsers.temporal.ParsedTemporalIssue.DATE_INVALID;
import static org.gbif.pipelines.core.parsers.temporal.ParsedTemporalIssue.DATE_MISMATCH;
import static org.gbif.pipelines.core.parsers.temporal.ParsedTemporalIssue.DATE_UNLIKELY;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.core.parsers.temporal.accumulator.ChronoAccumulatorConverter;
import org.gbif.pipelines.core.parsers.temporal.parser.ParserRawDateTime;
import org.gbif.pipelines.core.parsers.temporal.utils.DelimiterUtils;

/**
 * Main interpreter class. Interpreter for raw temporal period. The main method interpret two dates,
 * from and to
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalParser {

  private static final int ISSUE_SIZE = ParsedTemporalIssue.values().length;

  private static final BiFunction<ChronoAccumulator, Set<ParsedTemporalIssue>, Temporal>
      TEMPORAL_FUNC = (ca, deq) -> ChronoAccumulatorConverter.toTemporal(ca, deq).orElse(null);

  private static final Predicate<Temporal> HAS_DAY_FN =
      t -> t instanceof LocalDate || t instanceof LocalDateTime || t instanceof OffsetDateTime;
  private static final Predicate<Temporal> HAS_MONTH_FN =
      t -> HAS_DAY_FN.test(t) || t instanceof YearMonth;
  private static final Predicate<Temporal> HAS_YEAR_FN =
      t -> HAS_MONTH_FN.test(t) || t instanceof Year;

  public static ParsedTemporal parse(String rawDate) {
    return parse("", "", "", rawDate);
  }

  public static ParsedTemporal parse(
      String rawYear, String rawMonth, String rawDay, String rawDate) {
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

    // Parse event date
    ParsedTemporal eventDateParsed = parseEventDate(rawDate);

    return mergeParsedTemporal(yearMonthDayParsed, eventDateParsed);
  }

  private static ParsedTemporal parseYearMonthDayParsed(
      String rawYear, String rawMonth, String rawDay) {
    Set<ParsedTemporalIssue> issues = new HashSet<>(ISSUE_SIZE);
    ChronoAccumulator accum = ChronoAccumulator.from(rawYear, rawMonth, rawDay);
    // Convert year, month and day parsed values
    Temporal temporal = TEMPORAL_FUNC.apply(accum, issues);
    Year year = HAS_YEAR_FN.test(temporal) ? Year.from(temporal) : null;
    Month month = HAS_MONTH_FN.test(temporal) ? Month.from(temporal) : null;
    Integer day = HAS_DAY_FN.test(temporal) ? MonthDay.from(temporal).getDayOfMonth() : null;

    boolean hasIssue = issues.contains(DATE_INVALID) || issues.contains(DATE_UNLIKELY);

    return hasIssue
        ? ParsedTemporal.create(issues)
        : ParsedTemporal.create(year, month, day, temporal, issues);
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

    if (!fromAccum.getLastParsed().isPresent() && !isNullOrEmpty(rawFrom)) {
      issues.add(DATE_INVALID);
    }

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

  private static ParsedTemporal mergeParsedTemporal(
      ParsedTemporal yearMonthDayParsed, ParsedTemporal eventDateParsed) {

    if (!eventDateParsed.getFromOpt().isPresent() && !yearMonthDayParsed.getFromOpt().isPresent()) {
      return eventDateParsed.getIssues().isEmpty() ? yearMonthDayParsed : eventDateParsed;
    }

    if (!eventDateParsed.getFromOpt().isPresent()) {
      return yearMonthDayParsed;
    }

    if (eventDateParsed.getToOpt().isPresent()) {
      return mergeYmd(yearMonthDayParsed, eventDateParsed);
    }

    return mergeFromAndYmd(yearMonthDayParsed, eventDateParsed);
  }

  /** Merge from date from event and date from yaer-month-day */
  private static ParsedTemporal mergeFromAndYmd(
      ParsedTemporal yearMonthDayParsed, ParsedTemporal eventDateParsed) {

    Temporal fromTemporal = eventDateParsed.getFromOpt().orElse(null);

    if (yearMonthDayParsed.getFromOpt().isPresent()
        && yearMonthDayParsed.getFromDate().equals(fromTemporal)) {
      return yearMonthDayParsed;
    }

    if (fromTemporal == null) {
      return yearMonthDayParsed;
    }

    Year fromYear = HAS_YEAR_FN.test(fromTemporal) ? Year.from(fromTemporal) : null;
    Year year = yearMonthDayParsed.getYear();
    boolean isYearMatch = fromYear == null || year == null || fromYear.equals(year);
    Year yearNotNull = fromYear == null ? year : fromYear;
    Year resultYear = isYearMatch ? yearNotNull : year;

    Month fromMonth = HAS_MONTH_FN.test(fromTemporal) ? Month.from(fromTemporal) : null;
    Month month = yearMonthDayParsed.getMonth();
    boolean isMonthMatch = fromMonth == null || month == null || fromMonth.equals(month);
    Month monthNotNull = fromMonth == null ? month : fromMonth;
    Month resultMonth = isMonthMatch ? monthNotNull : month;

    Integer fromDay =
        HAS_DAY_FN.test(fromTemporal) ? MonthDay.from(fromTemporal).getDayOfMonth() : null;
    Integer day = yearMonthDayParsed.getDay();
    boolean isDayMatch = fromDay == null || day == null || fromDay.equals(day);
    Integer dayNotNull = fromDay == null ? day : fromDay;
    Integer resultDay = isDayMatch ? dayNotNull : day;

    // To support US format
    if (isYearMatch
        && !isMonthMatch
        && !isDayMatch
        && month.getValue() == fromDay
        && fromMonth.getValue() == day) {
      isMonthMatch = true;
      isDayMatch = true;
    }

    boolean hasTime =
        fromTemporal instanceof LocalDateTime || fromTemporal instanceof OffsetDateTime;
    LocalTime resultTime = hasTime ? LocalTime.from(fromTemporal) : null;

    ZoneOffset resultOffset =
        fromTemporal instanceof OffsetDateTime ? OffsetTime.from(fromTemporal).getOffset() : null;

    if (!isYearMatch || !isMonthMatch || !isDayMatch) {
      yearMonthDayParsed.setFromDate(fromTemporal);
      yearMonthDayParsed.setIssues(Collections.singleton(DATE_MISMATCH));
    } else {
      yearMonthDayParsed.setFromDate(resultYear, resultMonth, resultDay, resultTime, resultOffset);
    }

    yearMonthDayParsed.setYear(resultYear);
    yearMonthDayParsed.setMonth(resultMonth);
    yearMonthDayParsed.setDay(resultDay);

    return yearMonthDayParsed;
  }

  /** Merge from date from/to event and date from year-month-day */
  private static ParsedTemporal mergeYmd(
      ParsedTemporal yearMonthDayParsed, ParsedTemporal eventDateParsed) {
    // To support US format
    swapForUsFormat(yearMonthDayParsed, eventDateParsed);

    Temporal fromTemporal = eventDateParsed.getFromDate();
    Temporal toTemporal = eventDateParsed.getToDate();

    yearMonthDayParsed.setFromDate(fromTemporal);
    yearMonthDayParsed.setToDate(toTemporal);

    Year fromYear = Year.from(fromTemporal);
    if (!yearMonthDayParsed.getYearOpt().isPresent()) {
      yearMonthDayParsed.setYear(fromYear);
    } else if (!yearMonthDayParsed.getYear().equals(fromYear)) {
      yearMonthDayParsed.setIssues(Collections.singleton(DATE_MISMATCH));
      return yearMonthDayParsed;
    }

    Optional<Year> year = yearMonthDayParsed.getYearOpt();
    Month fromMonth = HAS_MONTH_FN.test(fromTemporal) ? Month.from(fromTemporal) : null;
    if (!yearMonthDayParsed.getMonthOpt().isPresent() && year.isPresent()) {
      yearMonthDayParsed.setMonth(fromMonth);
    } else if (!yearMonthDayParsed.getMonth().equals(fromMonth)) {
      yearMonthDayParsed.setIssues(Collections.singleton(DATE_MISMATCH));
      return yearMonthDayParsed;
    }

    Optional<Month> month = yearMonthDayParsed.getMonthOpt();
    Integer fromDay =
        HAS_DAY_FN.test(fromTemporal) ? MonthDay.from(fromTemporal).getDayOfMonth() : null;
    if (!yearMonthDayParsed.getDayOpt().isPresent() && month.isPresent()) {
      yearMonthDayParsed.setDay(fromDay);
    } else if (yearMonthDayParsed.getDayOpt().isPresent()
        && month.isPresent()
        && !yearMonthDayParsed.getDay().equals(fromDay)) {
      yearMonthDayParsed.setIssues(Collections.singleton(DATE_MISMATCH));
      return yearMonthDayParsed;
    }

    return yearMonthDayParsed;
  }

  /** Swap date and month if format is US */
  private static void swapForUsFormat(
      ParsedTemporal yearMonthDayParsed, ParsedTemporal eventDateParsed) {
    Temporal fromDate = eventDateParsed.getFromDate();
    Temporal toDate = eventDateParsed.getToDate();

    boolean containsDateMonthFrom =
        fromDate instanceof LocalDateTime
            || fromDate instanceof LocalDate
            || fromDate instanceof OffsetDateTime;

    boolean containsDateMonthTo =
        toDate instanceof LocalDateTime
            || toDate instanceof LocalDate
            || toDate instanceof OffsetDateTime;

    boolean containsDateMonthSimple =
        yearMonthDayParsed.getYearOpt().isPresent()
            && yearMonthDayParsed.getMonthOpt().isPresent()
            && yearMonthDayParsed.getDayOpt().isPresent();

    if (containsDateMonthFrom && containsDateMonthTo && containsDateMonthSimple) {
      if (!Month.from(fromDate).equals(yearMonthDayParsed.getMonth())
          && MonthDay.from(fromDate).getDayOfMonth() != yearMonthDayParsed.getDay()
          && Month.from(fromDate).getValue() == yearMonthDayParsed.getDay()
          && MonthDay.from(fromDate).getDayOfMonth() == yearMonthDayParsed.getMonth().getValue()) {

        Temporal fromTemporal = fromDate;
        Temporal toTemporal = toDate;
        if (fromDate instanceof OffsetDateTime && toDate instanceof OffsetDateTime) {
          OffsetDateTime ldtf = (OffsetDateTime) fromDate;
          OffsetDateTime ldtt = (OffsetDateTime) toDate;
          fromTemporal =
              OffsetDateTime.of(
                  LocalDateTime.of(
                      ldtf.getYear(),
                      ldtf.getDayOfMonth(),
                      ldtf.getMonth().getValue(),
                      ldtf.getHour(),
                      ldtf.getMinute(),
                      ldtf.getSecond()),
                  ldtf.getOffset());
          toTemporal =
              OffsetDateTime.of(
                  LocalDateTime.of(
                      ldtt.getYear(),
                      ldtt.getDayOfMonth(),
                      ldtt.getMonth().getValue(),
                      ldtt.getHour(),
                      ldtt.getMinute(),
                      ldtt.getSecond()),
                  ldtt.getOffset());
        } else if (fromDate instanceof LocalDateTime && toDate instanceof LocalDateTime) {
          LocalDateTime ldtf = (LocalDateTime) fromDate;
          LocalDateTime ldtt = (LocalDateTime) toDate;
          fromTemporal =
              LocalDateTime.of(
                  ldtf.getYear(),
                  ldtf.getDayOfMonth(),
                  ldtf.getMonth().getValue(),
                  ldtf.getHour(),
                  ldtf.getMinute(),
                  ldtf.getSecond());
          toTemporal =
              LocalDateTime.of(
                  ldtt.getYear(),
                  ldtt.getDayOfMonth(),
                  ldtt.getMonth().getValue(),
                  ldtt.getHour(),
                  ldtt.getMinute(),
                  ldtt.getSecond());
        } else if (fromDate instanceof LocalDate && toDate instanceof LocalDate) {
          LocalDate ldtf = (LocalDate) fromDate;
          LocalDate ldtt = (LocalDate) toDate;
          fromTemporal =
              LocalDate.of(ldtf.getYear(), ldtf.getDayOfMonth(), ldtf.getMonth().getValue());
          toTemporal =
              LocalDate.of(ldtt.getYear(), ldtt.getDayOfMonth(), ldtt.getMonth().getValue());
        }
        eventDateParsed.setFromDate(fromTemporal);
        eventDateParsed.setToDate(toTemporal);
      }
    }
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
    } else if (from instanceof LocalDateTime || from instanceof OffsetDateTime) {
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
}
