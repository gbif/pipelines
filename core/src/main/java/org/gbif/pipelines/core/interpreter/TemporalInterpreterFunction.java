package org.gbif.pipelines.core.interpreter;

import org.gbif.pipelines.core.interpreter.temporal.ParsedDate;
import org.gbif.pipelines.core.interpreter.temporal.ParsedTemporalDate;
import org.gbif.pipelines.core.interpreter.temporal.RawTemporalInterpreter;

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

  public static ParsedTemporalDate apply(CharSequence rDate) {
    String sDate = CONVERT_FUNC.apply(rDate);
    return apply(null, null, null, sDate);
  }

  public static ParsedTemporalDate apply(String rDate) {
    return apply(null, null, null, rDate);
  }

  public static ParsedTemporalDate apply(CharSequence year, CharSequence month, CharSequence day, CharSequence rDate) {
    String sYear = CONVERT_FUNC.apply(year);
    String sMonth = CONVERT_FUNC.apply(month);
    String sDay = CONVERT_FUNC.apply(day);
    String sEventDate = CONVERT_FUNC.apply(rDate);
    return apply(sYear, sMonth, sDay, sEventDate);
  }

  public static ParsedTemporalDate apply(String year, String month, String day, String rDate) {
    //If year and rDate are absent, return ParsedTemporalDate with NULL values inside
    if (isEmpty(year) && isEmpty(rDate)) {
      return new ParsedTemporalDate();
    }

    //Convert year, date and day to ParsedDate for next usage in interpretation
    ParsedDate baseDate = ParsedDate.of(year, month, day);

    if (isEmpty(rDate)) {
      return new ParsedTemporalDate(baseDate);
    }

    return RawTemporalInterpreter.interpret(baseDate, rDate);
  }

}
