package org.gbif.pipelines.interpretation.parsers.temporal;

import org.gbif.pipelines.interpretation.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.interpretation.parsers.temporal.accumulator.ChronoAccumulatorConverter;
import org.gbif.pipelines.interpretation.parsers.temporal.parser.ParserRawDateTime;
import org.gbif.pipelines.interpretation.parsers.temporal.utils.DelimiterUtils;

import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.function.Function;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Interpreter for raw temporal period. The main method interpret two dates, from and to
 */
public class InterpretRawPeriod {

  private static final Function<ChronoAccumulator, Temporal> CONVERTER =
    ca -> ChronoAccumulatorConverter.toTemporal(ca).orElse(null);

  private InterpretRawPeriod() {
    //NOP
  }

  public static ParsedTemporalDates interpret(String rawYear, String rawMonth, String rawDay, String rawDate) {
    //Fill base values
    ChronoAccumulator baseAccumulator = ChronoAccumulator.from(rawYear, rawMonth, rawDay);

    Year year = ChronoAccumulatorConverter.getYear(baseAccumulator).orElse(null);
    Month month = ChronoAccumulatorConverter.getMonth(baseAccumulator).orElse(null);
    Integer day = ChronoAccumulatorConverter.getDay(baseAccumulator).orElse(null);
    Temporal base = CONVERTER.apply(baseAccumulator);

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

    temporalDates.setFromDate(CONVERTER.apply(fromAccumulator));
    temporalDates.setToDate(CONVERTER.apply(toAccumulator));
    return temporalDates;
  }

}
