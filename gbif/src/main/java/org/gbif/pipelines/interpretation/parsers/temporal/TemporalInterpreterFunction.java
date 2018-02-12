package org.gbif.pipelines.interpretation.parsers.temporal;

import java.util.Optional;
import java.util.function.Function;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Main interpreter class
 */
public class TemporalInterpreterFunction {

  private static final Function<CharSequence, String> CONVERT_FUNC = cs -> Optional.ofNullable(cs).orElse("").toString();

  private TemporalInterpreterFunction() {
    //NOP
  }

  public static ParsedTemporalDates apply(CharSequence year, CharSequence month, CharSequence day, CharSequence rawDate) {
    String strYear = CONVERT_FUNC.apply(year);
    String strMonth = CONVERT_FUNC.apply(month);
    String strDay = CONVERT_FUNC.apply(day);
    String strEventDate = CONVERT_FUNC.apply(rawDate);
    return apply(strYear, strMonth, strDay, strEventDate);
  }

  public static ParsedTemporalDates apply(String year, String month, String day, String rawDate) {
    //If year and rawDate are absent, return ParsedTemporalDates with NULL values inside
    if (isEmpty(year) && isEmpty(rawDate)) {
      return new ParsedTemporalDates();
    }

    return InterpretRawPeriod.interpret(year, month, day, rawDate);
  }

}
