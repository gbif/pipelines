package org.gbif.pipelines.interpretation.parsers.temporal;

import java.time.temporal.ChronoField;
import java.util.regex.Pattern;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNumeric;

/**
 * Interpreter for raw date only. The main method interpret, fills year, month and day in ParseDate
 */
class InterpretRawDate {

  private static final int YEAR_LENGTH = 4;
  private static final String REPLACE_CHAR = " ";
  private static final Pattern RGX_FILTER = Pattern.compile("[^a-zA-Z0-9]+");
  private static final Pattern RGX_REPLACE_CHAR = Pattern.compile(REPLACE_CHAR);

  private InterpretRawDate() {
    //NOP
  }

  static ChronoStorage interpret(String rawDate) {
    return interpret(rawDate, ChronoField.ERA);
  }

  static ChronoStorage interpret(String rawDate, ChronoField lastParsed) {
    if (isEmpty(rawDate)) {
      return new ChronoStorage();
    }

    //Filter and split raw date
    String filtered = RGX_FILTER.matcher(rawDate).replaceAll(REPLACE_CHAR).trim();
    String[] input = RGX_REPLACE_CHAR.split(filtered);

    //Choose case based on split array length
    int length = input.length;
    if (length == 1) {
      return parseSizeOne(lastParsed, input);
    } else if (length == 2) {
      return parseSizeTwo(lastParsed, input);
    } else if (length == 3) {
      return parseSizeThree(input);
    }

    return new ChronoStorage();
  }

  private static boolean isYear(String year) {
    return year.length() == YEAR_LENGTH && isNumeric(year);
  }

  private static ChronoStorage parseSizeOne(ChronoField lastParsed, String... dateArray) {
    ChronoStorage chronoStorage = new ChronoStorage();
    String first = dateArray[0];
    //Case example - 20120506
    if (first.length() == 8 && isNumeric(first)) {
      String year = first.substring(0, 4);
      String month = first.substring(4, 6);
      String day = first.substring(6, 8);
      return parseSizeThree(year, month, day);
    }
    //If values is year
    boolean isYearFirst = isYear(first);
    if (isYearFirst) {
      chronoStorage.convertAndPut(YEAR, first);
      return chronoStorage;
    }
    //If it not a year, this array should represent toDate,
    //which may have month or day, determines by last parsed value in fromDate
    if (MONTH_OF_YEAR == lastParsed) {
      chronoStorage.convertAndPut(MONTH_OF_YEAR, first);
    } else if (DAY_OF_MONTH == lastParsed) {
      chronoStorage.convertAndPut(DAY_OF_MONTH, first);
    }
    return chronoStorage;
  }

  private static ChronoStorage parseSizeTwo(ChronoField lastParsed, String... dateArray) {
    ChronoStorage chronoStorage = new ChronoStorage();
    String first = dateArray[0];
    String second = dateArray[1];
    boolean isYearFirst = isYear(first);
    boolean isYearSecond = isYear(second);
    //If any of values is year, set year and month
    if (isYearFirst || isYearSecond) {
      chronoStorage.convertAndPut(YEAR, isYearFirst ? first : second);
      chronoStorage.convertAndPut(MONTH_OF_YEAR, isYearFirst ? second : first);
    } else {
      //If year is absent, this array should represent toDate,
      //which may have month and day, determines by last parsed value in fromDate
      boolean isMonthFirst = DAY_OF_MONTH == lastParsed;
      chronoStorage.convertAndPut(MONTH_OF_YEAR, isMonthFirst ? first : second);
      chronoStorage.convertAndPut(DAY_OF_MONTH, isMonthFirst ? second : first);
    }
    return chronoStorage;
  }

  private static ChronoStorage parseSizeThree(String... dateArray) {
    ChronoStorage chronoStorage = new ChronoStorage();
    String first = dateArray[0];
    String second = dateArray[1];
    String third = dateArray[2];

    boolean isYearFirst = isYear(first);

    //Parse year
    String year = isYearFirst ? first : third;

    //Parse month
    String positionOne = isYearFirst ? second : first;
    String positionTwo = isYearFirst ? third : second;

    //Check month value non-numeric, as JUNE or etc.
    boolean isFirst = !isNumeric(positionOne);
    boolean isSecond = !isNumeric(positionTwo);

    //If both values non-numeric, month should be in the middle
    String month;
    if (isFirst || isSecond) {
      month = isFirst ? positionOne : positionTwo;
    } else {
      month = second;
    }

    //Parse day
    String position = isYearFirst ? third : first;
    String day = month.equals(second) ? position : second;

    //Save results
    chronoStorage.convertAndPut(YEAR, year);
    chronoStorage.convertAndPut(MONTH_OF_YEAR, month);
    chronoStorage.convertAndPut(DAY_OF_MONTH, day);
    return chronoStorage;
  }

}