package org.gbif.pipelines.parsers.parsers.temporal;

import org.gbif.pipelines.parsers.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.parsers.parsers.temporal.accumulator.ChronoAccumulatorConverter;
import org.gbif.pipelines.parsers.parsers.temporal.parser.ParserRawDateTime;
import org.gbif.pipelines.parsers.parsers.temporal.utils.DelimiterUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_INVALID;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Main interpreter class. Interpreter for raw temporal period. The main method interpret two dates,
 * from and to
 */
public class TemporalParser {

  private static final BiFunction<ChronoAccumulator, List<String>, Temporal> TEMPORAL_FUNC =
      (ca, deq) -> ChronoAccumulatorConverter.toTemporal(ca, deq).orElse(null);

  private TemporalParser() {
    // Can't have an instance
  }

  public static ParsedTemporalDates parse(String rawDate) {
    return parse("", "", "", rawDate);
  }

  public static ParsedTemporalDates parse(
      String rawYear, String rawMonth, String rawDay, String rawDate) {
    // If year and rawDate are absent, return ParsedTemporalDates with NULL values inside
    if (isNullOrEmpty(rawYear) && isNullOrEmpty(rawDate)) {
      return new ParsedTemporalDates();
    }

    List<String> issueList = new ArrayList<>();

    // Parse year, month and day
    ChronoAccumulator baseAccumulator = ChronoAccumulator.from(rawYear, rawMonth, rawDay);

    // Convert year, month and day parsed values
    Year year = ChronoAccumulatorConverter.getYear(baseAccumulator, issueList).orElse(null);
    Month month = ChronoAccumulatorConverter.getMonth(baseAccumulator, issueList).orElse(null);
    Integer day = ChronoAccumulatorConverter.getDay(baseAccumulator, issueList).orElse(null);
    Temporal base = TEMPORAL_FUNC.apply(baseAccumulator, issueList);

    // Base temporal instance
    ParsedTemporalDates temporalDates = new ParsedTemporalDates(year, month, day, base);

    if (isNullOrEmpty(rawDate)) {
      return temporalDates;
    }

    // Parse period
    String[] rawPeriod = DelimiterUtils.splitPeriod(rawDate);
    String rawFrom = rawPeriod[0];
    String rawTo = rawPeriod[1];
    ChronoAccumulator fromAccumulator = ParserRawDateTime.parse(rawFrom, null);
    ChronoAccumulator toAccumulator =
        ParserRawDateTime.parse(rawTo, fromAccumulator.getLastParsed().orElse(null));

    if (fromAccumulator.areAllNumeric()
        || (!isNullOrEmpty(rawTo) && toAccumulator.areAllNumeric())) {
      issueList.add(RECORDED_DATE_INVALID.name());
    }

    // If toAccumulator doesn't contain last parsed value, raw date will consist of one date only
    if (toAccumulator.getLastParsed().isPresent()) {
      // Use toAccumulator value toAccumulator improve fromAccumulator parsed date
      toAccumulator.mergeAbsent(fromAccumulator);
    } else {
      // Use baseAccumulator value toAccumulator improve parsed date
      fromAccumulator.mergeReplace(baseAccumulator);
    }

    Temporal fromTemporal = TEMPORAL_FUNC.apply(fromAccumulator, issueList);
    Temporal toTemporal = TEMPORAL_FUNC.apply(toAccumulator, issueList);

    if (!isValidDateType(fromTemporal, toTemporal)) {
      toTemporal = null;
      issueList.add(RECORDED_DATE_INVALID.name());
    }

    if (!isValidRange(fromTemporal, toTemporal)) {
      Temporal tmp = fromTemporal;
      fromTemporal = toTemporal;
      toTemporal = tmp;
      issueList.add("DAY_OUT_OF_RANGE");
    }

    temporalDates.setFromDate(fromTemporal);
    temporalDates.setToDate(toTemporal);
    temporalDates.setIssueList(issueList);
    return temporalDates;
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
