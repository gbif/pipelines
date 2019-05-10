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
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import org.gbif.pipelines.parsers.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.parsers.parsers.temporal.accumulator.ChronoAccumulatorConverter;
import org.gbif.pipelines.parsers.parsers.temporal.parser.ParserRawDateTime;
import org.gbif.pipelines.parsers.parsers.temporal.utils.DelimiterUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_INVALID;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Main interpreter class. Interpreter for raw temporal period. The main method interpret two dates,
 * from and to
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalParser {

  private static final BiFunction<ChronoAccumulator, List<String>, Temporal> TEMPORAL_FUNC =
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
      return new ParsedTemporal();
    }

    List<String> issueList = new ArrayList<>();

    // Parse year, month and day
    ChronoAccumulator accum = ChronoAccumulator.from(rawYear, rawMonth, rawDay);

    ParsedTemporal temporalDates = getBaseParsedTemporal(accum, issueList);

    if (isNullOrEmpty(rawDate)) {
      return temporalDates;
    }

    // Parse period
    String[] rawPeriod = DelimiterUtils.splitPeriod(rawDate);
    String rawFrom = rawPeriod[0];
    String rawTo = rawPeriod[1];

    ChronoAccumulator fromAccum = ParserRawDateTime.parse(rawFrom, null);

    ChronoField lastChronoField = fromAccum.getLastParsed().orElse(null);
    ChronoAccumulator toAccum = ParserRawDateTime.parse(rawTo, lastChronoField);

    if (fromAccum.areAllNumeric() || (!isNullOrEmpty(rawTo) && toAccum.areAllNumeric())) {
      issueList.add(RECORDED_DATE_INVALID.name());
    }

    // If toAccum doesn't contain last parsed value, raw date will consist of one date only
    if (toAccum.getLastParsed().isPresent()) {
      // Use toAccum value toAccum improve fromAccum parsed date
      toAccum.mergeAbsent(fromAccum);
    } else {
      // Use accum value toAccum improve parsed date
      fromAccum.mergeReplace(accum);
    }

    Temporal fromTemporal = TEMPORAL_FUNC.apply(fromAccum, issueList);
    Temporal toTemporal = TEMPORAL_FUNC.apply(toAccum, issueList);

    if (!isValidDateType(fromTemporal, toTemporal)) {
      toTemporal = null;
      issueList.add(RECORDED_DATE_INVALID.name());
    }

    if (!isValidRange(fromTemporal, toTemporal)) {
      Temporal tmp = fromTemporal;
      fromTemporal = toTemporal;
      toTemporal = tmp;
      issueList.add(RECORDED_DATE_INVALID.name());
    }

    if (fromTemporal != null) {
      if (!temporalDates.getYear().isPresent() && HAS_YEAR_FN.test(fromTemporal)) {
        temporalDates.setYear(Year.from(fromTemporal));
      }
      if (!temporalDates.getMonth().isPresent() && HAS_MONTH_FN.test(fromTemporal) && HAS_YEAR_MATCH.test(fromTemporal,
          temporalDates.getYear().get())) {
        temporalDates.setMonth(Month.from(fromTemporal));
      }
      if (!temporalDates.getDay().isPresent() && HAS_DAY_FN.test(fromTemporal) && HAS_MONTH_MATCH.test(fromTemporal,
          temporalDates.getMonth().get())) {
        temporalDates.setDay(MonthDay.from(fromTemporal).getDayOfMonth());
      }
    }

    temporalDates.setFromDate(fromTemporal);
    temporalDates.setToDate(toTemporal);
    temporalDates.setIssueList(issueList);
    return temporalDates;
  }

  private static ParsedTemporal getBaseParsedTemporal(ChronoAccumulator accumulator, List<String> issueList) {
    // Convert year, month and day parsed values
    Year year = ChronoAccumulatorConverter.getYear(accumulator, issueList).orElse(null);
    Month month = ChronoAccumulatorConverter.getMonth(accumulator, issueList).orElse(null);
    Integer day = ChronoAccumulatorConverter.getDay(accumulator, issueList).orElse(null);
    Temporal base = TEMPORAL_FUNC.apply(accumulator, issueList);

    // Base temporal instance
    return new ParsedTemporal(year, month, day, base);
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
}
