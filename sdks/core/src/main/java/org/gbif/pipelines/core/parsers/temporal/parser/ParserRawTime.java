package org.gbif.pipelines.core.parsers.temporal.parser;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.core.parsers.temporal.utils.DelimiterUtils;

/** Interpreter for raw time. The main method parse */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class ParserRawTime {

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
    String[] timeZoneArray = DelimiterUtils.splitTimeAndZone(rawTime);
    String[] timeArray = DelimiterUtils.splitTime(timeZoneArray[0]);

    // Parse time only
    if (timeArray.length > 1) {
      accumulator.setChronoField(HOUR_OF_DAY, timeArray[0]);
      accumulator.setChronoField(MINUTE_OF_HOUR, timeArray[1]);
      if (timeArray.length > 2) {
        accumulator.setChronoField(SECOND_OF_MINUTE, timeArray[2]);
      }
    }

    // Commented to support API v1, see issue -> https://github.com/gbif/gbif-api/issues/3
    // Can be uncommented for API v2
    // if (timeZoneArray.length == 2 && timeZoneArray[1].length() > 1) {
    //  accumulator.setChronoField(OFFSET_SECONDS, timeZoneArray[1]);
    // }

    return accumulator;
  }
}
