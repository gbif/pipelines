package org.gbif.pipelines.core.parsers.temporal.parser;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNumeric;

import java.time.temporal.ChronoField;
import java.util.function.Predicate;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.core.parsers.temporal.accumulator.ChronoAccumulator;
import org.gbif.pipelines.core.parsers.temporal.utils.DelimiterUtils;

/** Interpreter for raw date only. The main method parse, fills year, month and day in ParseDate */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class ParserRawDate {

  private static final Predicate<String> YEAR_PREDICATE =
      year -> year.length() == 4 && isNumeric(year);

  /**
   * Parse year, month and day index in the raw date string, and save raw values into
   * ChronoAccumulator
   *
   * @param rawDate raw date string
   * @param lastParsed if it is date "to", it can store only one value
   * @return ChronoAccumulator which store all parsed values
   */
  public static ChronoAccumulator parse(String rawDate, ChronoField lastParsed) {
    if (isEmpty(rawDate)) {
      return new ChronoAccumulator();
    }

    // Filter and split raw date
    String[] input = DelimiterUtils.splitDate(rawDate);

    // Choose case based on split array length
    int length = input.length;
    if (length == 1) {
      return parseSizeOne(lastParsed, input);
    }
    if (length == 2) {
      return parseSizeTwo(lastParsed, input);
    }
    if (length == 3) {
      return parseSizeThree(input);
    }

    return new ChronoAccumulator();
  }

  private static ChronoAccumulator parseSizeOne(ChronoField lastParsed, String... dateArray) {
    ChronoAccumulator accumulator = new ChronoAccumulator();
    String first = dateArray[0];
    // Case example - 20120506
    if (first.length() == 8 && isNumeric(first)) {
      String year = first.substring(0, 4);
      String month = first.substring(4, 6);
      String day = first.substring(6, 8);
      return parseSizeThree(year, month, day);
    }
    // If values is year
    boolean isYearFirst = YEAR_PREDICATE.test(first);
    if (isYearFirst) {
      accumulator.setChronoField(YEAR, first);
      return accumulator;
    }
    // If it not a year, this array should represent toDate,
    // which may have month or day, determines by last parsed value in fromDate
    if (MONTH_OF_YEAR == lastParsed) {
      accumulator.setChronoField(MONTH_OF_YEAR, first);
    } else if (DAY_OF_MONTH == lastParsed) {
      accumulator.setChronoField(DAY_OF_MONTH, first);
    }
    return accumulator;
  }

  private static ChronoAccumulator parseSizeTwo(ChronoField lastParsed, String... dateArray) {
    ChronoAccumulator accumulator = new ChronoAccumulator();
    String first = dateArray[0];
    String second = dateArray[1];
    boolean isYearFirst = YEAR_PREDICATE.test(first);
    boolean isYearSecond = YEAR_PREDICATE.test(second);
    // If any of values is year, set year and month
    if (isYearFirst || isYearSecond) {
      accumulator.setChronoField(YEAR, isYearFirst ? first : second);
      accumulator.setChronoField(MONTH_OF_YEAR, isYearFirst ? second : first);
    } else {
      // If year is absent, this array should represent toDate,
      // which may have month and day, determines by last parsed value in fromDate
      boolean isMonthFirst = DAY_OF_MONTH == lastParsed;
      accumulator.setChronoField(MONTH_OF_YEAR, isMonthFirst ? first : second);
      accumulator.setChronoField(DAY_OF_MONTH, isMonthFirst ? second : first);
    }
    return accumulator;
  }

  private static ChronoAccumulator parseSizeThree(String... dateArray) {
    ChronoAccumulator accumulator = new ChronoAccumulator();
    String first = dateArray[0];
    String second = dateArray[1];
    String third = dateArray[2];

    boolean isYearFirst = YEAR_PREDICATE.test(first);

    // Parse year
    String year = isYearFirst ? first : third;

    // Parse month
    String positionOne = isYearFirst ? second : first;
    String positionTwo = isYearFirst ? third : second;

    // Check month value non-numeric, as JUNE or etc.
    boolean isFirst = !isNumeric(positionOne);
    boolean isSecond = !isNumeric(positionTwo);

    // If both values non-numeric, month should be in the middle
    String month;
    if (isFirst || isSecond) {
      month = isFirst ? positionOne : positionTwo;
    } else {
      month = second;
    }

    // Parse day
    String position = isYearFirst ? third : first;
    String day = month.equals(second) ? position : second;

    // Save results
    accumulator.setChronoField(YEAR, year);
    accumulator.setChronoField(MONTH_OF_YEAR, month);
    accumulator.setChronoField(DAY_OF_MONTH, day);
    return accumulator;
  }
}
