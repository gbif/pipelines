package org.gbif.pipelines.core.parsers.temporal;

import static org.gbif.common.parsers.core.ParseResult.CONFIDENCE.PROBABLE;

import com.google.common.base.Strings;
import com.google.common.collect.Range;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.CustomizedTextDateParser;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.common.parsers.date.DateParsers;
import org.gbif.common.parsers.date.TemporalAccessorUtils;

@Slf4j
public class TemporalParser implements Serializable {

  private static final long serialVersionUID = -8845127337324812802L;

  private static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1500, 1, 1);

  private final org.gbif.common.parsers.date.TemporalParser temporalParser;

  private TemporalParser(List<DateComponentOrdering> orderings) {
    if (orderings != null && !orderings.isEmpty()) {
      DateComponentOrdering[] array = orderings.toArray(new DateComponentOrdering[0]);
      temporalParser = CustomizedTextDateParser.getInstance(array);
    } else {
      temporalParser = DateParsers.defaultTemporalParser();
    }
  }

  public static TemporalParser create(List<DateComponentOrdering> orderings) {
    return new TemporalParser(orderings);
  }

  public static TemporalParser create() {
    return create(Collections.emptyList());
  }

  public OccurrenceParseResult<TemporalAccessor> parseRecordedDate(
      String year, String month, String day, String dateString) {
    return parseRecordedDate(year, month, day, dateString, null);
  }

  /**
   * Three dates are provided:
   *
   * <ul>
   *   <li>year, month and day
   *   <li>dateString
   *   <li>year and dayOfYear
   * </ul>
   *
   * <p>Produces a single date at the best resolution possible, ignoring missing values.
   *
   * <p>Years are verified to be before this year and after 1500.
   *
   * @return interpretation result, never null
   */
  public OccurrenceParseResult<TemporalAccessor> parseRecordedDate(
      String year, String month, String day, String dateString, String dayOfYear) {


    boolean ymdProvided =
        StringUtils.isNotBlank(year)
            || StringUtils.isNotBlank(month)
            || StringUtils.isNotBlank(day);
    boolean dateStringProvided = StringUtils.isNotBlank(dateString);
    boolean yDoyProvided = StringUtils.isNotBlank(year) && StringUtils.isNotBlank(dayOfYear);

    if (!ymdProvided && !dateStringProvided && !yDoyProvided) {
      return OccurrenceParseResult.fail();
    }

    Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);

    TemporalAccessor parsedTemporalAccessor;
    ParseResult.CONFIDENCE confidence;

    // Parse all three possible dates
    ParseResult<TemporalAccessor> parsedYMDResult =
        ymdProvided ? temporalParser.parse(year, month, day) : ParseResult.fail();
    ParseResult<TemporalAccessor> parsedDateResult =
        dateStringProvided ? temporalParser.parse(dateString) : ParseResult.fail();
    ParseResult<TemporalAccessor> parsedYearDoyResult =
        yDoyProvided ? temporalParser.parse(year, dayOfYear) : ParseResult.fail();
    TemporalAccessor parsedYmdTa = parsedYMDResult.getPayload();
    TemporalAccessor parsedDateTa = parsedDateResult.getPayload();
    TemporalAccessor parsedYearDoyTa = parsedYearDoyResult.getPayload();

    int ymdResolution = -1, dateStringResolution = -1;
    if (ymdProvided && parsedYMDResult.isSuccessful()) {
      ymdResolution = TemporalAccessorUtils.resolution(parsedYmdTa);
    }
    if (dateStringProvided && parsedDateResult.isSuccessful()) {
      dateStringResolution = TemporalAccessorUtils.resolution(parsedDateTa);
    }

    // Add issues if we failed to parse any dates that were present
    if (ymdProvided && !parsedYMDResult.isSuccessful()) {
      issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
    }
    if (dateStringProvided && !parsedDateResult.isSuccessful()) {
      issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
    }
    if (yDoyProvided && !parsedYearDoyResult.isSuccessful()) {
      issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
    }

    // If a dateString is provided with something else, handle the case where it doesn't match.
    boolean ambiguityResolved = false;
    if (ymdProvided
        && dateStringProvided
        && !TemporalAccessorUtils.sameOrContained(parsedYmdTa, parsedDateTa)
        && parsedDateResult.getAlternativePayloads() != null) {

      // eventDate could be ambiguous (5/4/2014), but disambiguated by year-month-day.
      Optional<TemporalAccessor> resolved =
          TemporalAccessorUtils.resolveAmbiguousDates(
              parsedYmdTa, parsedDateResult.getAlternativePayloads());
      if (resolved.isPresent()) {
        parsedDateTa = resolved.get();
        ambiguityResolved = true;
        log.debug(
            "Ambiguous date {} matches year-month-day date {}-{}-{} for {}",
            dateString,
            year,
            month,
            day,
            parsedDateTa);
      }
      // still a conflict
      if (!ambiguityResolved) {
        if (parsedYmdTa == null || parsedDateTa == null) {
          issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
        } else {
          issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);
        }
      }
    } else if (ymdProvided
        && yDoyProvided
        && !TemporalAccessorUtils.sameOrContained(parsedYearDoyTa, parsedDateTa)
        && parsedDateResult.getAlternativePayloads() != null) {

      // eventDate could be ambiguous (5/4/2014), but disambiguated by year-month-day.
      Optional<TemporalAccessor> resolved =
          TemporalAccessorUtils.resolveAmbiguousDates(
              parsedYearDoyTa, parsedDateResult.getAlternativePayloads());
      if (resolved.isPresent()) {
        parsedDateTa = resolved.get();
        ambiguityResolved = true;
        log.debug(
            "Ambiguous date {} matches year-dayOfYear date {}-{} for {}",
            dateString,
            year,
            dayOfYear,
            parsedDateTa);
      }
      // still a conflict
      if (!ambiguityResolved) {
        if (parsedYmdTa == null || parsedYearDoyTa == null) {
          issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
        } else {
          issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);
        }
      }
    }

    // Add an issue if there is any conflict between the dates
    if (TemporalAccessorUtils.sameOrContainedOrNull(parsedYmdTa, parsedDateTa)
        && TemporalAccessorUtils.sameOrContainedOrNull(parsedYmdTa, parsedYearDoyTa)
        && TemporalAccessorUtils.sameOrContainedOrNull(parsedDateTa, parsedYearDoyTa)) {
      confidence =
          parsedDateTa != null
              ? parsedDateResult.getConfidence()
              : (parsedYmdTa != null
                  ? parsedYMDResult.getConfidence()
                  : parsedYearDoyResult.getConfidence());
    } else {
      issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);
      confidence = PROBABLE;
    }

    // Add an issue if the resolution af ymd / date / yDoy is different
    if (ymdResolution > 0 && dateStringResolution > 0) {
      if (ymdResolution != dateStringResolution) {
        issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);
      }
    }

    // Best we can get from the three parts.
    // Note 2000-01-01 and 2000-01 and 2000 will return 2000-01-01.
    Optional<TemporalAccessor> nonConflictingTa =
        TemporalAccessorUtils.nonConflictingDateParts(parsedYmdTa, parsedDateTa, parsedYearDoyTa);

    if (nonConflictingTa.isPresent()) {
      parsedTemporalAccessor = nonConflictingTa.get();
      // if one of the parses failed we can not set the confidence to DEFINITE
      confidence =
          ((ymdProvided && parsedYmdTa == null)
                  || (dateStringProvided && parsedDateTa == null)
                  || (yDoyProvided && parsedYearDoyTa == null))
              ? PROBABLE
              : confidence;
    } else {
      issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
      return OccurrenceParseResult.fail(issues);
    }

    if (!isValidDate(parsedTemporalAccessor)) {
      if (parsedTemporalAccessor == null) {
        issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
      } else {
        issues.add(OccurrenceIssue.RECORDED_DATE_UNLIKELY);
      }

      log.debug("Invalid date: [{}]].", parsedTemporalAccessor);
      return OccurrenceParseResult.fail(issues);
    }

    return OccurrenceParseResult.success(confidence, parsedTemporalAccessor, issues);
  }

  public OccurrenceParseResult<TemporalAccessor> parseRecordedDate(String dateString) {
    return parseRecordedDate(null, null, null, dateString, null);
  }

  /** @return TemporalAccessor that represents a LocalDate or LocalDateTime */
  public OccurrenceParseResult<TemporalAccessor> parseLocalDate(
      String dateString, Range<LocalDate> likelyRange, OccurrenceIssue unlikelyIssue) {
    if (!Strings.isNullOrEmpty(dateString)) {
      OccurrenceParseResult<TemporalAccessor> result =
          new OccurrenceParseResult<>(temporalParser.parse(dateString));
      // check year makes sense
      if (result.isSuccessful() && !isValidDate(result.getPayload(), likelyRange)) {
        log.debug("Unlikely date parsed, ignore [{}].", dateString);
        result.addIssue(unlikelyIssue);
      }
      return result;
    }
    return OccurrenceParseResult.fail();
  }

  /**
   * Check if a date express as TemporalAccessor falls between the predefined range. Lower bound
   * defined by {@link #MIN_LOCAL_DATE} and upper bound by current date + 1 day
   *
   * @return valid or not according to the predefined range.
   */
  protected static boolean isValidDate(TemporalAccessor temporalAccessor) {
    LocalDate upperBound = LocalDate.now().plusDays(1);
    return isValidDate(temporalAccessor, Range.closed(MIN_LOCAL_DATE, upperBound));
  }

  /** Check if a date express as TemporalAccessor falls between the provided range. */
  protected static boolean isValidDate(
      TemporalAccessor temporalAccessor, Range<LocalDate> likelyRange) {

    if (temporalAccessor == null) {
      return false;
    }

    // if partial dates should be considered valid
    int year;
    int month = 1;
    int day = 1;
    if (temporalAccessor.isSupported(ChronoField.YEAR)) {
      year = temporalAccessor.get(ChronoField.YEAR);
    } else {
      return false;
    }

    if (temporalAccessor.isSupported(ChronoField.MONTH_OF_YEAR)) {
      month = temporalAccessor.get(ChronoField.MONTH_OF_YEAR);
    }

    if (temporalAccessor.isSupported(ChronoField.DAY_OF_MONTH)) {
      day = temporalAccessor.get(ChronoField.DAY_OF_MONTH);
    }

    return likelyRange.contains(LocalDate.of(year, month, day));
  }
}
