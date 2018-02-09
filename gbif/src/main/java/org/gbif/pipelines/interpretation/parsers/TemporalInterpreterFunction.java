package org.gbif.pipelines.interpretation.parsers;

import org.gbif.pipelines.interpretation.parsers.temporal.InterpretRawPeriod;
import org.gbif.pipelines.interpretation.parsers.temporal.ParsedTemporalPeriod;

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

  public static ParsedTemporalPeriod apply(CharSequence rDate) {
    String sDate = CONVERT_FUNC.apply(rDate);
    return apply(null, null, null, sDate);
  }

  public static ParsedTemporalPeriod apply(String rDate) {
    return apply(null, null, null, rDate);
  }

  public static ParsedTemporalPeriod apply(int year, int month, int day, String rDate) {
    String sDate = CONVERT_FUNC.apply(rDate);
    return apply(String.valueOf(year), String.valueOf(month), String.valueOf(day), sDate);
  }

  public static ParsedTemporalPeriod apply(int year, int month, int day, CharSequence rDate) {
    return apply(String.valueOf(year), String.valueOf(month), String.valueOf(day), rDate);
  }

  public static ParsedTemporalPeriod apply(CharSequence year, CharSequence month, CharSequence day, CharSequence rDate) {
    String sYear = CONVERT_FUNC.apply(year);
    String sMonth = CONVERT_FUNC.apply(month);
    String sDay = CONVERT_FUNC.apply(day);
    String sEventDate = CONVERT_FUNC.apply(rDate);
    return apply(sYear, sMonth, sDay, sEventDate);
  }

  public static ParsedTemporalPeriod apply(String year, String month, String day, String rawDate) {
    //If year and rawDate are absent, return ParsedTemporalPeriod with NULL values inside
    if (isEmpty(year) && isEmpty(rawDate)) {
      return new ParsedTemporalPeriod();
    }

    return InterpretRawPeriod.interpret(year, month, day, rawDate);
  }

}
