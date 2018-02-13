package org.gbif.pipelines.interpretation.parsers.temporal;

import org.gbif.pipelines.interpretation.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.interpretation.parsers.temporal.accumulator.ChronoAccumulatorConverter;
import org.gbif.pipelines.interpretation.parsers.temporal.parser.ParserRawDateTime;
import org.gbif.pipelines.interpretation.parsers.temporal.utils.DelimiterUtils;

import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Main interpreter class. Interpreter for raw temporal period. The main method interpret two dates, from and to
 */
public class TemporalInterpreterFunction {

  private static final Function<ChronoAccumulator, Temporal> TEMPORAL_FUNC = ca -> ChronoAccumulatorConverter.toTemporal(ca).orElse(null);
  private static final Function<CharSequence, String> CHAR_SEQUE_FUNC = cs -> Optional.ofNullable(cs).orElse("").toString();

  private TemporalInterpreterFunction() {
    // Can't have an instance
  }

   public static ParsedTemporalDates apply(CharSequence year, CharSequence month, CharSequence day, CharSequence rawDate) {
    String strYear = CHAR_SEQUE_FUNC.apply(year);
    String strMonth = CHAR_SEQUE_FUNC.apply(month);
    String strDay = CHAR_SEQUE_FUNC.apply(day);
    String strEventDate = CHAR_SEQUE_FUNC.apply(rawDate);
    return apply(strYear, strMonth, strDay, strEventDate);
  }

  public static ParsedTemporalDates apply(String rawYear, String rawMonth, String rawDay, String rawDate) {
    //If year and rawDate are absent, return ParsedTemporalDates with NULL values inside
    if (isEmpty(rawYear) && isEmpty(rawDate)) {
      return new ParsedTemporalDates();
    }

    //Fill base values
    ChronoAccumulator baseAccumulator = ChronoAccumulator.from(rawYear, rawMonth, rawDay);

    Year year = ChronoAccumulatorConverter.getYear(baseAccumulator).orElse(null);
    Month month = ChronoAccumulatorConverter.getMonth(baseAccumulator).orElse(null);
    Integer day = ChronoAccumulatorConverter.getDay(baseAccumulator).orElse(null);
    Temporal base = TEMPORAL_FUNC.apply(baseAccumulator);

    ParsedTemporalDates temporalDates = new ParsedTemporalDates(year, month, day, base);

    if (isEmpty(rawDate)) {
      return temporalDates;
    }

    String[] rawPeriod = DelimiterUtils.splitPeriod(rawDate);

    ChronoAccumulator fromAccumulator = ParserRawDateTime.parse(rawPeriod[0], baseAccumulator.getLastParsed());
    ChronoAccumulator toAccumulator = ParserRawDateTime.parse(rawPeriod[1], fromAccumulator.getLastParsed());

    // If toAccumulator doesn't contain last parsed value, raw date will consist of one date only
    if (toAccumulator.getLastParsed() == null) {
      // Use baseAccumulator value toAccumulator improve parsed date
      fromAccumulator.putAllAndReplce(baseAccumulator);
    } else {
      // Use toAccumulator value toAccumulator improve fromAccumulator parsed date
      toAccumulator.putAllIfAbsent(fromAccumulator);
    }

    temporalDates.setFromDate(TEMPORAL_FUNC.apply(fromAccumulator));
    temporalDates.setToDate(TEMPORAL_FUNC.apply(toAccumulator));
    return temporalDates;
  }

}
