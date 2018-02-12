package org.gbif.pipelines.interpretation.parsers.temporal;

import java.time.temporal.ChronoField;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Interpreter for raw date and time. The main method interpret, fills year, month, day, hour, minute, second and time zone
 */
class InterpretRawDateTime {

  private static final char TIME_SPLITERATOR = ':';
  private static final Pattern RGX_YEAR = Pattern.compile("[0-9]{4}");
  private static final Pattern RGX_PATTERN = Pattern.compile("[0-9]{2}");

  private InterpretRawDateTime() {
    //NOP
  }

  static ChronoAccumulator interpret(String rawDate, ChronoField lastParsed) {
    if (isEmpty(rawDate) || (!RGX_YEAR.matcher(rawDate).find() && !RGX_PATTERN.matcher(rawDate).find())) {
      return new ChronoAccumulator();
    }

    //Does value have time inside
    int timeSpliterIdx = rawDate.indexOf(TIME_SPLITERATOR);
    boolean hasTime = timeSpliterIdx > 0;

    //Calculate a shift index of the time, for case as 09:00 or 9:00
    int shift = -1;
    for (int x = timeSpliterIdx - 1; x > 0; x--) {
      char sign = rawDate.charAt(x);
      if (!Character.isDigit(sign)) {
        shift = x;
        break;
      }
    }

    //Calculate the beginning index of date, the date can be absent, in this case, substring from 0 to 0 returns empty string
    int dateIdx = shift < 0 ? 0 : shift;
    String date = hasTime ? rawDate.substring(0, dateIdx) : rawDate;

    //Calculate the beginning index of time
    int timeIdx = shift + 1;
    String time = hasTime ? rawDate.substring(timeIdx) : "";

    //Interpret date and time separately
    ChronoAccumulator temporalDate = InterpretRawDate.interpret(date, lastParsed);
    ChronoAccumulator temporalTime = InterpretRawTime.interpret(time);
    return temporalDate.merge(temporalTime);
  }

}
