package org.gbif.pipelines.core.interpreter.temporal;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Function;

import static org.gbif.pipelines.core.interpreter.temporal.ParsedDateConstant.BASE_ZONE_ID;
import static org.gbif.pipelines.core.interpreter.temporal.ParsedDateConstant.ISSUE;
import static org.gbif.pipelines.core.interpreter.temporal.ParsedDateConstant.ParsedElementEnum.*;
import static org.gbif.pipelines.core.interpreter.temporal.ParsedDateConstant.ParsedElementEnum;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Base class represented parsed date with additional methods for parsing every value
 * All values initialized by default, year value is ISSUE(-1)
 */
public class ParsedDate {

  private static final Integer BASE_DATE_MOUNTH = 1;
  private static final Integer BASE_TIME = 0;

  protected int year = ISSUE;
  protected int month = BASE_DATE_MOUNTH;
  protected int day = BASE_DATE_MOUNTH;
  protected int hour = BASE_TIME;
  protected int minute = BASE_TIME;
  protected int second = BASE_TIME;
  protected ZoneId zoneId = BASE_ZONE_ID;

  private boolean hasIssue = false;
  private ParsedElementEnum parsedEnum = NONE;

  public static ParsedDate copyOrCreate(ParsedDate pDate) {
    ParsedDate copy = new ParsedDate();
    if (pDate == null) {
      return copy;
    }
    copy.year = pDate.year;
    copy.month = pDate.month;
    copy.day = pDate.day;
    copy.hour = pDate.hour;
    copy.minute = pDate.minute;
    copy.second = pDate.second;
    copy.zoneId = pDate.zoneId;
    copy.parsedEnum = pDate.parsedEnum;
    copy.hasIssue = pDate.hasIssue;
    return copy;
  }

  public static ParsedDate of(String year, String month, String day) {
    ParsedDate parsedDate = new ParsedDate();
    parsedDate.parseAndSetYear(year);
    parsedDate.parseAndSetMonth(month);
    parsedDate.parseAndSetDay(day);
    return parsedDate;
  }

  public ZonedDateTime toZonedDateTime() {
    return ParsedDateUtils.toZonedDateTime(this);
  }

  public boolean hasIssue() {
    return hasIssue;
  }

  public ParsedElementEnum getParsedEnum() {
    return parsedEnum;
  }

  /**
   * @return null, if year has issue
   */
  public String toStringOrNull() {
    return this.year <= 1 ? null : toZonedDateTime().toString();
  }

  @Override
  public String toString() {
    return toZonedDateTime().toString();
  }

  void parseAndSetYear(String rYear) {
    this.year = parseRawValue(year, rYear, YEAR, ParsedDateUtils::parseYear);
  }

  void parseAndSetMonth(String rMonth) {
    this.month = parseRawValue(month, rMonth, MONTH, ParsedDateUtils::parseMonth);
  }

  void parseAndSetDay(String rDay) {
    this.day = parseRawValue(day, rDay, DAY, ParsedDateUtils::parseDay);
  }

  void parseAndSetHour(String rHour) {
    this.hour = parseRawValue(hour, rHour, HOUR, ParsedDateUtils::parseHour);
  }

  void parseAndSetMinute(String rMinute) {
    this.minute = parseRawValue(minute, rMinute, MINUTE, ParsedDateUtils::parseMinute);
  }

  void parseAndSetSecond(String rSecond) {
    this.second = parseRawValue(second, rSecond, SECOND, ParsedDateUtils::parseSecond);
  }

  void parseAndSetZone(String zone) {
    this.zoneId = parseRawValue(zoneId, zone, ZONE, ParsedDateUtils::parseZone);
  }

  /**
   * Common method for processing all actions with raw values
   * @param bValue base value, for parsed value with issue
   * @param rValue raw value for parsing
   * @param parsedEnum store last parsed value
   * @param func main function for parsing
   * @return parsed value or base value if any issue during the parsing process
   */
  private <T> T parseRawValue(T bValue, String rValue, ParsedElementEnum parsedEnum, Function<String, T> func) {
    if (isEmpty(rValue)) {
      return bValue;
    }
    T value = func.apply(rValue);
    this.parsedEnum = parsedEnum;
    if (ISSUE.equals(value)) {
      this.hasIssue = true;
      return bValue;
    }
    return value;
  }
}
