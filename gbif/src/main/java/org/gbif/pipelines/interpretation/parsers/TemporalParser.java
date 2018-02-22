package org.gbif.pipelines.interpretation.parsers;

import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.interpretation.parsers.temporal.ParsedTemporalDates;
import org.gbif.pipelines.interpretation.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.interpretation.parsers.temporal.accumulator.ChronoAccumulatorConverter;
import org.gbif.pipelines.interpretation.parsers.temporal.parser.ParserRawDateTime;
import org.gbif.pipelines.interpretation.parsers.temporal.utils.DelimiterUtils;

import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Main interpreter class. Interpreter for raw temporal period. The main method interpret two dates, from and to
 */
public class TemporalParser {

  private static final BiFunction<ChronoAccumulator, List<Issue>, Temporal> TEMPORAL_FUNC =
    (ca, deq) -> ChronoAccumulatorConverter.toTemporal(ca, deq).orElse(null);

  private TemporalParser() {
    // Can't have an instance
  }

  public static ParsedTemporalDates parse(String rawYear, String rawMonth, String rawDay, String rawDate) {
    // If year and rawDate are absent, return ParsedTemporalDates with NULL values inside
    if (isEmpty(rawYear) && isEmpty(rawDate)) {
      return new ParsedTemporalDates();
    }

    List<Issue> issueList = new ArrayList<>();

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
      issueList.add(Issue.newBuilder().setIssueType(IssueType.RECORDED_DATE_INVALID).build());
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

    temporalDates.setFromDate(fromTemporal);
    temporalDates.setToDate(toTemporal);
    temporalDates.setIssueList(issueList);
    return temporalDates;
  }

}
