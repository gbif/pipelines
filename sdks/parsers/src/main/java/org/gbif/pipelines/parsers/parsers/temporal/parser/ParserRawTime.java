package org.gbif.pipelines.parsers.parsers.temporal.parser;

import org.gbif.pipelines.parsers.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.parsers.parsers.temporal.utils.DelimiterUtils;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import static com.google.common.base.Strings.isNullOrEmpty;

/** Interpreter for raw time. The main method parse */
class ParserRawTime {

  private ParserRawTime() {
    // Can't have an instance
  }

  /**
   * Parse hour, minute and second position in the raw date string, and save raw values into
   * ChronoAccumulator
   *
   * @param rawTime raw time string
   * @return ChronoAccumulator which store all parsed values
   */
  public static ChronoAccumulator parse(String rawTime) {
    ChronoAccumulator accumulator = new ChronoAccumulator();
    if (isNullOrEmpty(rawTime)) {
      return accumulator;
    }
    // Split by some zone char
    String[] timeArray = DelimiterUtils.splitTime(rawTime);

    // Parse time only
    if (timeArray.length > 1) {
      accumulator.setChronoField(HOUR_OF_DAY, timeArray[0]);
      accumulator.setChronoField(MINUTE_OF_HOUR, timeArray[1]);
      if (timeArray.length > 2) {
        accumulator.setChronoField(SECOND_OF_MINUTE, timeArray[2]);
      }
    }

    return accumulator;
  }
}
