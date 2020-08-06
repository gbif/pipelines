package org.gbif.pipelines.core.interpreters.core;

import static org.gbif.common.parsers.core.ParseResult.CONFIDENCE.DEFINITE;
import static org.gbif.common.parsers.core.ParseResult.CONFIDENCE.PROBABLE;
import static org.gbif.pipelines.core.utils.ModelUtils.addIssueSet;
import static org.gbif.pipelines.core.utils.ModelUtils.extractValue;
import static org.gbif.pipelines.core.utils.ModelUtils.hasValue;

import com.google.common.base.Strings;
import com.google.common.collect.Range;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.AtomizedLocalDate;
import org.gbif.common.parsers.date.DateParsers;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/** Interprets date representations into a Date to support API v1 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalInterpreter {

  private static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1600, 1, 1);
  private static final LocalDate MIN_EPOCH_LOCAL_DATE = LocalDate.ofEpochDay(0);

  private static final TemporalParser TEXTDATE_PARSER = DateParsers.defaultTemporalParser();

  public static void interpretTemporal(ExtendedRecord er, TemporalRecord tr) {
    OccurrenceParseResult<TemporalAccessor> eventResult = interpretRecordedDate(er);
    if (eventResult.isSuccessful()) {
      TemporalAccessor temporalAccessor = eventResult.getPayload();

      // Get eventDate as java.util.Date and ignore the offset (timezone) if provided
      // Note for debug: be careful if you inspect the content of 'eventDate' it will contain your
      // machine timezone.
      LocalDateTime eventDate =
          TemporalAccessorUtils.toEarliestLocalDateTime(temporalAccessor, true);
      AtomizedLocalDate atomizedLocalDate =
          AtomizedLocalDate.fromTemporalAccessor(temporalAccessor);

      Optional.ofNullable(eventDate)
          .map(LocalDateTime::toString)
          .ifPresent(x -> tr.setEventDate(new EventDate(x, null)));

      tr.setYear(atomizedLocalDate.getYear());
      tr.setMonth(atomizedLocalDate.getMonth());
      tr.setDay(atomizedLocalDate.getDay());
    }
    addIssueSet(tr, eventResult.getIssues());

    LocalDate upperBound = LocalDate.now().plusDays(1);
    if (hasValue(er, DcTerm.modified)) {
      Range<LocalDate> validModifiedDateRange = Range.closed(MIN_EPOCH_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed =
          interpretLocalDate(
              extractValue(er, DcTerm.modified),
              validModifiedDateRange,
              OccurrenceIssue.MODIFIED_DATE_UNLIKELY);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(
                TemporalAccessorUtils.toEarliestLocalDateTime(parsed.getPayload(), false))
            .map(LocalDateTime::toString)
            .ifPresent(tr::setModified);
      }

      addIssueSet(tr, parsed.getIssues());
    }

    if (hasValue(er, DwcTerm.dateIdentified)) {
      Range<LocalDate> validRecordedDateRange = Range.closed(MIN_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed =
          interpretLocalDate(
              extractValue(er, DwcTerm.dateIdentified),
              validRecordedDateRange,
              OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY);
      if (parsed.isSuccessful()) {
        Optional.ofNullable(
                TemporalAccessorUtils.toEarliestLocalDateTime(parsed.getPayload(), false))
            .map(LocalDateTime::toString)
            .ifPresent(tr::setDateIdentified);
      }
      addIssueSet(tr, parsed.getIssues());
    }
  }

  /**
   * A convenience method that calls interpretRecordedDate with the verbatim recordedDate values
   * from the VerbatimOccurrence.
   *
   * @param er the VerbatimOccurrence containing a recordedDate
   * @return the interpretation result which is never null
   */
  public static OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(ExtendedRecord er) {
    final String year = extractValue(er, DwcTerm.year);
    final String month = extractValue(er, DwcTerm.month);
    final String day = extractValue(er, DwcTerm.day);
    final String dateString = extractValue(er, DwcTerm.eventDate);

    return interpretRecordedDate(year, month, day, dateString);
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
  public static OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(
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
        atomizedDateProvided ? TEXTDATE_PARSER.parse(year, month, day) : ParseResult.fail();
    ParseResult<TemporalAccessor> parsedDateResult =
        dateStringProvided ? TEXTDATE_PARSER.parse(dateString) : ParseResult.fail();
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
        issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);
        log.debug("Date mismatch: [{} vs {}].", parsedYmdTa, parsedDateTa);
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

    if (!isValidDate(parsedTemporalAccessor, true)) {
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

  /**
   * Check if a date express as TemporalAccessor falls between the predefined range. Lower bound
   * defined by {@link #MIN_LOCAL_DATE} and upper bound by current date + 1 day
   *
   * @return valid or not according to the predefined range.
   */
  public static boolean isValidDate(TemporalAccessor temporalAccessor, boolean acceptPartialDate) {
    LocalDate upperBound = LocalDate.now().plusDays(1);
    return isValidDate(
        temporalAccessor, acceptPartialDate, Range.closed(MIN_LOCAL_DATE, upperBound));
  }

  /** Check if a date express as TemporalAccessor falls between the provided range. */
  public static boolean isValidDate(
      TemporalAccessor temporalAccessor, boolean acceptPartialDate, Range<LocalDate> likelyRange) {

    if (temporalAccessor == null) {
      return false;
    }

    if (!acceptPartialDate) {
      LocalDate localDate = temporalAccessor.query(TemporalQueries.localDate());
      if (localDate == null) {
        return false;
      }
      return likelyRange.contains(localDate);
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

  /** @return TemporalAccessor that represents a LocalDate or LocalDateTime */
  public static OccurrenceParseResult<TemporalAccessor> interpretLocalDate(
      String dateString, Range<LocalDate> likelyRange, OccurrenceIssue unlikelyIssue) {
    if (!Strings.isNullOrEmpty(dateString)) {
      OccurrenceParseResult<TemporalAccessor> result =
          new OccurrenceParseResult<>(TEXTDATE_PARSER.parse(dateString));
      // check year makes sense
      if (result.isSuccessful() && !isValidDate(result.getPayload(), true, likelyRange)) {
        log.debug("Unlikely date parsed, ignore [{}].", dateString);
        result.addIssue(unlikelyIssue);
      }
      return result;
    }
    return OccurrenceParseResult.fail();
  }
}
