package org.gbif.pipelines.core.parsers.temporal;

import org.gbif.pipelines.core.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.core.parsers.temporal.accumulator.ChronoAccumulatorConverter;
import org.gbif.pipelines.core.parsers.temporal.parser.ParserRawDateTime;
import org.gbif.pipelines.core.parsers.temporal.utils.DelimiterUtils;
import org.gbif.pipelines.io.avro.IssueType;

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
import java.util.Objects;
import java.util.function.BiFunction;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Main interpreter class. Interpreter for raw temporal period. The main method interpret two dates, from and to
 */
public class TemporalParser {

  private static final BiFunction<ChronoAccumulator, List<IssueType>, Temporal> TEMPORAL_FUNC =
    (ca, deq) -> ChronoAccumulatorConverter.toTemporal(ca, deq).orElse(null);

  private TemporalParser() {
    // Can't have an instance
  }

  public static ParsedTemporalDates parse(String rawDate) {
    return parse("", "", "", rawDate);
  }

  public static ParsedTemporalDates parse(String rawYear, String rawMonth, String rawDay, String rawDate) {
    // If year and rawDate are absent, return ParsedTemporalDates with NULL values inside
    if (isEmpty(rawYear) && isEmpty(rawDate)) {
      return new ParsedTemporalDates();
    }

    List<IssueType> issueList = new ArrayList<>();

    // Parse year, month and day
    ChronoAccumulator baseAccumulator = ChronoAccumulator.from(rawYear, rawMonth, rawDay);

    // Convert year, month and day parsed values
    Year year = ChronoAccumulatorConverter.getYear(baseAccumulator, issueList).orElse(null);
    Month month = ChronoAccumulatorConverter.getMonth(baseAccumulator, issueList).orElse(null);
    Integer day = ChronoAccumulatorConverter.getDay(baseAccumulator, issueList).orElse(null);
    Temporal base = TEMPORAL_FUNC.apply(baseAccumulator, issueList);

    // Base temporal instance
    ParsedTemporalDates temporalDates = new ParsedTemporalDates(year, month, day, base);

    if (isEmpty(rawDate)) {
      return temporalDates;
    }

    // Parse period
    String[] rawPeriod = DelimiterUtils.splitPeriod(rawDate);
    String rawFrom = rawPeriod[0];
    String rawTo = rawPeriod[1];
    ChronoAccumulator fromAccumulator = ParserRawDateTime.parse(rawFrom, null);
    ChronoAccumulator toAccumulator = ParserRawDateTime.parse(rawTo, fromAccumulator.getLastParsed().orElse(null));

    if (fromAccumulator.areAllNumeric() || (!isEmpty(rawTo) && toAccumulator.areAllNumeric())) {
      issueList.add(IssueType.RECORDED_DATE_INVALID);
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

    if (isValidRange(fromTemporal, toTemporal)) {
      Temporal tmp = fromTemporal;
      fromTemporal = toTemporal;
      toTemporal = tmp;
      issueList.add(IssueType.DAY_OUT_OF_RANGE);
    }

    temporalDates.setFromDate(fromTemporal);
    temporalDates.setToDate(toTemporal);
    temporalDates.setIssueList(issueList);
    return temporalDates;
  }

  /**
   * Compare dates, FROM cannot be greater than TO
   */
  private static boolean isValidRange(Temporal from, Temporal to) {
    if (Objects.isNull(from) || Objects.isNull(to)) {
      return false;
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
    return from.until(to, unit) < 0;
  }
}
