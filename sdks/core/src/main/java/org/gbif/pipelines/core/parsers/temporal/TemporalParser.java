package org.gbif.pipelines.core.parsers.temporal;

import static org.gbif.common.parsers.core.ParseResult.CONFIDENCE.DEFINITE;
import static org.gbif.common.parsers.core.ParseResult.CONFIDENCE.PROBABLE;

import com.google.common.base.Strings;
import com.google.common.collect.Range;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.*;
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

  /**
   * Given possibly both of year, month, day and a dateString, produces a single date. When year,
   * month and day are all populated and parseable they are given priority, but if any field is
   * missing or illegal and dateString is parseable dateString is preferred. Partially valid dates
   * are not supported and null will be returned instead. The only exception is the year alone which
   * will be used as the last resort if nothing else works. Years are verified to be before or next
   * year and after 1600. x
   *
   * @return interpretation result, never null
   */
  public OccurrenceParseResult<TemporalAccessor> parseRecordedDate(
      String year, String month, String day, String dateString) {

    boolean atomizedDateProvided =
        StringUtils.isNotBlank(year)
            || StringUtils.isNotBlank(month)
            || StringUtils.isNotBlank(day);
    boolean dateStringProvided = StringUtils.isNotBlank(dateString);

    if (!atomizedDateProvided && !dateStringProvided) {
      return OccurrenceParseResult.fail();
    }

    Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);

    // First, attempt year, month, day parsing
    // If the parse result is SUCCESS it means that a whole date could be extracted (with year,
    // month and day). If it is a failure but the normalizer returned a meaningful result (e.g. it
    // could extract just
    // a year) we're going to return a result with all the fields set that we could parse.
    TemporalAccessor parsedTemporalAccessor;
    ParseResult.CONFIDENCE confidence;

    ParseResult<TemporalAccessor> parsedYMDResult =
        atomizedDateProvided ? temporalParser.parse(year, month, day) : ParseResult.fail();
    ParseResult<TemporalAccessor> parsedDateResult =
        dateStringProvided ? temporalParser.parse(dateString) : ParseResult.fail();
    TemporalAccessor parsedYmdTa = parsedYMDResult.getPayload();
    TemporalAccessor parsedDateTa = parsedDateResult.getPayload();

    // If both inputs exist handle the case when they don't match
    if (atomizedDateProvided
        && dateStringProvided
        && !TemporalAccessorUtils.sameOrContained(parsedYmdTa, parsedDateTa)) {

      // eventDate could be ambiguous (5/4/2014), but disambiguated by year-month-day.
      boolean ambiguityResolved = false;
      if (parsedDateResult.getAlternativePayloads() != null) {
        for (TemporalAccessor possibleTa : parsedDateResult.getAlternativePayloads()) {
          if (TemporalAccessorUtils.sameOrContained(parsedYmdTa, possibleTa)) {
            parsedDateTa = possibleTa;
            ambiguityResolved = true;
            log.debug(
                "Ambiguous date {} matches year-month-day date {}-{}-{} for {}",
                dateString,
                year,
                month,
                day,
                parsedDateTa);
          }
        }
      }

      // still a conflict
      if (!ambiguityResolved) {
        if (parsedYmdTa == null || parsedDateTa == null) {
          issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
        } else {
          issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);
        }
      }

      // choose the one with better resolution
      Optional<TemporalAccessor> bestResolution =
          TemporalAccessorUtils.bestResolution(parsedYmdTa, parsedDateTa);
      if (bestResolution.isPresent()) {
        parsedTemporalAccessor = bestResolution.get();
        // if one of the two results is null we can not set the confidence to DEFINITE
        confidence = (parsedYmdTa == null || parsedDateTa == null) ? PROBABLE : DEFINITE;
      } else {
        return OccurrenceParseResult.fail(issues);
      }
    } else {
      // they match, or we only have one anyway, choose the one with better resolution.
      parsedTemporalAccessor =
          TemporalAccessorUtils.bestResolution(parsedYmdTa, parsedDateTa).orElse(null);
      confidence =
          parsedDateTa != null ? parsedDateResult.getConfidence() : parsedYMDResult.getConfidence();
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
    return parseRecordedDate(null, null, null, dateString);
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
