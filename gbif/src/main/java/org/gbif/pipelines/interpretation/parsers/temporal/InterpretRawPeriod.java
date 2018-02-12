package org.gbif.pipelines.interpretation.parsers.temporal;

import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Interpreter for raw temporal period. The main method interpret two dates, from and to
 */
public class InterpretRawPeriod {

  private static final String CHAR_PERIOD = "/";
  private static final Pattern RGX_PERIOD = Pattern.compile(CHAR_PERIOD);

  private InterpretRawPeriod() {
    //NOP
  }

  public static ParsedTemporalPeriod interpret(String year, String month, String day, String rawDate) {
    ChronoAccumulator base = ChronoAccumulator.from(year, month, day);
    if (isEmpty(rawDate)) {
      return new ParsedTemporalPeriod(base.toTemporal());
    }

    String[] rawPeriod = splitByPeriod(rawDate);

    ChronoAccumulator from = InterpretRawDateTime.interpret(rawPeriod[0], base.getLastParsed());
    ChronoAccumulator to = InterpretRawDateTime.interpret(rawPeriod[1], from.getLastParsed());

    // If "to" doesn't contain last parsed value, raw date will consist of one date only
    if (to.getLastParsed() == null) {
      // Use base value to improve parsed date
      from.putAll(base);
    } else {
      // Use "to" value to improve "from" parsed date
      to.putAllIfAbsent(from);
    }

    return new ParsedTemporalPeriod(from.toTemporal(), to.toTemporal());

  }

  /**
   * Attempt to split the rawDate into raw periods by '/' symbol, the symbol must be the only one in the rawDate
   *
   * @param rawDate raw string date
   *
   * @return always two elements array
   */
  private static String[] splitByPeriod(String rawDate) {
    //If the spliterator for the first position and last position are the same, symbol only one in string,
    //check length, the length must be greater than seven to avoid case as "1999/2", where it looks as year and month
    boolean canSplit = rawDate.lastIndexOf(CHAR_PERIOD) == rawDate.indexOf(CHAR_PERIOD) && rawDate.length() > 7;
    String[] splited = canSplit ? RGX_PERIOD.split(rawDate) : new String[] {rawDate, ""};
    //Returns an array of the same length each time
    return splited.length < 2 ? new String[] {splited[0], ""} : splited;
  }

}
