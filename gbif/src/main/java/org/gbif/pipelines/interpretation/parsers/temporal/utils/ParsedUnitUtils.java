package org.gbif.pipelines.interpretation.parsers.temporal.utils;

import java.text.DateFormatSymbols;
import java.time.Year;
import java.util.function.Predicate;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNumeric;

/**
 * Util class for parsing values
 */
public class ParsedUnitUtils {

  private ParsedUnitUtils() {
    // Can't have an instance
  }

  public static Integer parseYear(String year) {
    return parseInteger(year, x -> x > Year.now().getValue() || x < 1000);
  }

  public static Integer parseMonth(String month) {
    if (isEmpty(month)) {
      return null;
    }
    return isNumeric(month) ? parseMonthAsInt(month) : parseMonthAsString(month);
  }

  public static Integer parseDay(String day) {
    return parseInteger(day, x -> x < 1 || x > 31);
  }

  public static Integer parseHour(String hour) {
    return parseInteger(hour, x -> x < 0 || x > 23);
  }

  public static Integer parseMinute(String minute) {
    return parseInteger(minute, x -> x < 0 || x > 59);
  }

  public static Integer parseSecond(String second) {
    return parseInteger(second, x -> x < 0 || x > 59);
  }

  private static Integer parseMonthAsString(String month) {
    Integer intMonth = null;
    String[] months = DateFormatSymbols.getInstance().getMonths();
    for (int x = 0; x < months.length; x++) {
      if (months[x].toLowerCase().startsWith(month.toLowerCase())) {
        intMonth = x + 1;
        break;
      }
    }
    return intMonth;
  }

  private static Integer parseMonthAsInt(String month) {
    return parseInteger(month, x -> x < 1 || x > 12);
  }

  /**
   * Common method for parsing shor numeric string to int
   *
   * @param rawValue  raw value for parsing
   * @param validator predica with validity conditions
   *
   * @return parsed value or ISSUE(-1) value, if value is invalid
   */
  private static Integer parseInteger(String rawValue, Predicate<Integer> validator) {
    Integer value = (!isEmpty(rawValue) && isNumeric(rawValue) && rawValue.length() < 5) ? Integer.valueOf(rawValue) : -1;
    return validator.test(value) ? null : value;
  }

}