package org.gbif.pipelines.core.interpreter.temporal;

import static org.gbif.pipelines.core.interpreter.temporal.ParsedDateConstant.ParsedElementEnum.DAY;
import static org.gbif.pipelines.core.interpreter.temporal.ParsedDateConstant.ParsedElementEnum.MONTH;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNumeric;

/**
 * Interpreter for raw date only. The main method interpret, fills year, month and day in ParseDate
 */
public class RawDateInterpreter {

  private static final int YEAR_LENGTH = 4;
  private static final String RGX_FILTER = "[^a-zA-Z0-9]+";
  private static final String RGX_REPLACE_CHAR = " ";

  private RawDateInterpreter() {
    //NOP
  }

  public static ParsedDate interpret(ParsedDate pDate, String rDate) {
    ParsedDate parsedDate = ParsedDate.copy(pDate);
    if (isEmpty(rDate)) {
      return parsedDate;
    }

    //Filter and split raw date
    String[] input = rDate.replaceAll(RGX_FILTER, RGX_REPLACE_CHAR).trim().split(RGX_REPLACE_CHAR);

    //Choose case based on split array length
    int length = input.length;
    if (length == 1) {
      parseSizeOne(parsedDate, input);
    } else if (length == 2) {
      parseSizeTwo(parsedDate, input);
    } else if (length == 3) {
      parseSizeThree(parsedDate, input);
    }

    return parsedDate;
  }

  private static boolean isYear(String year) {
    return year.length() == YEAR_LENGTH && isNumeric(year);
  }

  private static void parseSizeOne(ParsedDate pDate, String... aDate) {
    String first = aDate[0];
    //Case - 20120506
    if (first.length() == 8 && isNumeric(first)) {
      String year = first.substring(0, 4);
      String month = first.substring(4, 6);
      String day = first.substring(6, 8);
      parseSizeThree(pDate, year, month, day);
      return;
    }
    //If values is year
    boolean isYearFirst = isYear(first);
    if (isYearFirst) {
      pDate.parseAndSetYear(first);
      return;
    }
    //If it not a year, this array should represent toDate,
    //which may have month or day, determines by last parsed value in fromDate
    if (MONTH == pDate.getParsedEnum()) {
      pDate.parseAndSetMonth(first);
    } else if (DAY == pDate.getParsedEnum()) {
      pDate.parseAndSetDay(first);
    }
  }

  private static void parseSizeTwo(ParsedDate pDate, String... aDate) {
    String first = aDate[0];
    String second = aDate[1];
    boolean isYearFirst = isYear(first);
    boolean isYearSecond = isYear(second);
    //If any of values is year, set year and month
    if (isYearFirst || isYearSecond) {
      pDate.parseAndSetYear(isYearFirst ? first : second);
      pDate.parseAndSetMonth(isYearFirst ? second : first);
    } else {
      //If year is absent, this array should represent toDate,
      //which may have month and day, determines by last parsed value in fromDate
      boolean isMonthFirst = DAY == pDate.getParsedEnum();
      pDate.parseAndSetMonth(isMonthFirst ? first : second);
      pDate.parseAndSetDay(isMonthFirst ? second : first);
    }
  }

  private static void parseSizeThree(ParsedDate pDate, String... aDate) {
    boolean isYearFirst = isYear(aDate[0]);

    //Parse year
    int yearIdx = isYearFirst ? 0 : 2;
    pDate.parseAndSetYear(aDate[yearIdx]);

    //Parse month
    int pOne = isYearFirst ? 1 : 0;
    int pTwo = pOne + 1;

    //Check month value non-numeric, as JUNE or etc.
    boolean isFirst = !isNumeric(aDate[pOne]);
    boolean isSecond = !isNumeric(aDate[pTwo]);

    //If both values non-numeric, month should be in the middle
    int monthPosition;
    if (isFirst || isSecond) {
      monthPosition = isFirst ? pOne : pTwo;
    } else {
      monthPosition = 1;
    }
    pDate.parseAndSetMonth(aDate[monthPosition]);

    //parse day
    int position = isYearFirst ? 2 : 0;
    int dayPosition = monthPosition == 1 ? position : 1;
    pDate.parseAndSetDay(aDate[dayPosition]);
  }

}