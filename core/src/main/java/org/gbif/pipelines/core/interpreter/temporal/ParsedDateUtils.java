package org.gbif.pipelines.core.interpreter.temporal;

import java.text.DateFormatSymbols;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.core.interpreter.temporal.ParsedDateConstant.BASE_ZONE_ID;
import static org.gbif.pipelines.core.interpreter.temporal.ParsedDateConstant.ISSUE;

import static org.apache.commons.lang3.StringUtils.isNumeric;

/**
 * Util class for parsing values
 */
public class ParsedDateUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ParsedDate.class);

  private static final Integer MIN_YEAR = 1000;
  private static final ZonedDateTime BASE_DATE = LocalDateTime.of(1, 1, 1, 0, 0).atZone(BASE_ZONE_ID);

  private ParsedDateUtils() {
    //NOP
  }

  /**
   * Converts ParsedDate to ZonedDateTime, always returns valid ZonedDateTime
   *
   * @return if ZonedDateTime is invalid, will return BASE_DATE(0001-01-01T00:00Z)
   */
  public static ZonedDateTime toZonedDateTime(ParsedDate parsedDate) {
    boolean isValidDay = YearMonth.of(parsedDate.year, parsedDate.month).isValidDay(parsedDate.day);
    if (!isValidDay) {
      return BASE_DATE;
    }

    Function<Integer, Integer> convertIssue = x -> ISSUE.equals(x) ? 1 : x;

    Integer year = convertIssue.apply(parsedDate.year);
    Integer month = convertIssue.apply(parsedDate.month);
    Integer day = convertIssue.apply(parsedDate.day);
    Integer hour = convertIssue.apply(parsedDate.hour);
    Integer minute = convertIssue.apply(parsedDate.minute);
    Integer second = convertIssue.apply(parsedDate.second);

    return LocalDateTime.of(year, month, day, hour, minute, second).atZone(parsedDate.zoneId);
  }

  static Integer parseYear(String year) {
    return parseInteger(year, x -> x > Year.now().getValue() || x < MIN_YEAR);
  }

  static Integer parseMonth(String month) {
    if (isNumeric(month)) {
      return parseInteger(month, x -> x < 1 || x > 12);
    }

    String[] months = DateFormatSymbols.getInstance().getMonths();
    for (int x = 0; x < months.length; x++) {
      if (months[x].toLowerCase().startsWith(month.toLowerCase())) {
        return x + 1;
      }
    }
    return ISSUE;
  }

  static Integer parseDay(String day) {
    return parseInteger(day, x -> x < 1 || x > 31);
  }

  static Integer parseHour(String hour) {
    return parseInteger(hour, x -> x < 0 || x > 23);
  }

  static Integer parseMinute(String minute) {
    return parseInteger(minute, x -> x < 0 || x > 59);
  }

  static Integer parseSecond(String second) {
    return parseInteger(second, x -> x < 0 || x > 59);
  }

  static ZoneId parseZone(String zone) {
    try {
      return ZoneId.of(zone);
    } catch (DateTimeException ex) {
      LOG.warn(ex.getMessage());
    }
    return BASE_ZONE_ID;
  }

  /**
   * Common method for parsing shor numeric string to int
   *
   * @param rValue    raw value for parsing
   * @param validator predica with validity conditions
   *
   * @return parsed value or ISSUE(-1) value, if value is invalid
   */
  private static Integer parseInteger(String rValue, Predicate<Integer> validator) {
    Integer value = (isNumeric(rValue) && rValue.length() < 5) ? Integer.valueOf(rValue) : ISSUE;
    return validator.test(value) ? ISSUE : value;
  }

}