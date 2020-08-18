package org.gbif.pipelines.core.interpreters.core;

import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import com.google.common.collect.Range;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static lombok.AccessLevel.PRIVATE;

/** Interprets date representations into a Date to support API v1 */
@Slf4j

@NoArgsConstructor(access = PRIVATE)
public class TemporalInterpreter {
  private final static DefaultTemporalInterpreter temporalInterpreter = DefaultTemporalInterpreter.getInstance();

  public static void interpretTemporal(ExtendedRecord er, TemporalRecord tr){
    temporalInterpreter.interpretTemporal(er, tr);
  }

  /**
   * Given possibly both of year, month, day and a dateString, produces a single date.
   * When year, month and day are all populated and parseable they are given priority,
   * but if any field is missing or illegal and dateString is parseable dateString is preferred.
   * Partially valid dates are not supported and null will be returned instead. The only exception is the year alone
   * which will be used as the last resort if nothing else works.
   * Years are verified to be before or next year and after 1600.
   * x
   *
   * @return interpretation result, never null
   */
  public static  OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(String year, String month, String day,
      String dateString) {
    return  temporalInterpreter.interpretRecordedDate(year,month, day,dateString);
  }

  public static OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(ExtendedRecord er){
    return temporalInterpreter.interpretRecordedDate(er);
  }

  /**
   * @return TemporalAccessor that represents a LocalDate or LocalDateTime
   */
  public static  OccurrenceParseResult<TemporalAccessor> interpretLocalDate(String dateString,
      Range<LocalDate> likelyRange,
      OccurrenceIssue unlikelyIssue) {
    return temporalInterpreter.interpretLocalDate(dateString, likelyRange, unlikelyIssue);
  }

  /**
   * Check if a date express as TemporalAccessor falls between the predefined range.
   *  *
   * @return valid or not according to the predefined range.
   */
  public static boolean isValidDate(TemporalAccessor temporalAccessor, boolean acceptPartialDate) {
    return DefaultTemporalInterpreter.isValidDate(temporalAccessor,acceptPartialDate);
  }

  /**
   * Check if a date express as TemporalAccessor falls between the provided range.
   */
  public static boolean isValidDate(TemporalAccessor temporalAccessor, boolean acceptPartialDate,
      Range<LocalDate> likelyRange) {
    return DefaultTemporalInterpreter.isValidDate(temporalAccessor,acceptPartialDate,likelyRange);
  }

}