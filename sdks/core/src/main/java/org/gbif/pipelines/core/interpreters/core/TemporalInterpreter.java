package org.gbif.pipelines.core.interpreters.core;

import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjusters;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue;
import org.gbif.pipelines.parsers.parsers.temporal.TemporalParser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import static org.gbif.api.vocabulary.OccurrenceIssue.IDENTIFIED_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY;
import static org.gbif.api.vocabulary.OccurrenceIssue.MODIFIED_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.MODIFIED_DATE_UNLIKELY;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_INVALID;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_MISMATCH;
import static org.gbif.api.vocabulary.OccurrenceIssue.RECORDED_DATE_UNLIKELY;
import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_INVALID;
import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_MISMATCH;
import static org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporalIssue.DATE_UNLIKELY;
import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

/** Interprets the date related terms of a {@link ExtendedRecord}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TemporalInterpreter {

  private static final Map<ParsedTemporalIssue, String> EVENT_DATE_ISSUE_MAP = new EnumMap<>(ParsedTemporalIssue.class);
  private static final Map<ParsedTemporalIssue, String> MODIFIED_DATE_ISSUE_MAP = new EnumMap<>(ParsedTemporalIssue.class);
  private static final Map<ParsedTemporalIssue, String> IDENTIFIED_DATE_ISSUE_MAP = new EnumMap<>(ParsedTemporalIssue.class);

  static {
    EVENT_DATE_ISSUE_MAP.put(DATE_INVALID, RECORDED_DATE_INVALID.name());
    EVENT_DATE_ISSUE_MAP.put(DATE_UNLIKELY, RECORDED_DATE_UNLIKELY.name());
    EVENT_DATE_ISSUE_MAP.put(DATE_MISMATCH, RECORDED_DATE_MISMATCH.name());

    MODIFIED_DATE_ISSUE_MAP.put(DATE_UNLIKELY, MODIFIED_DATE_UNLIKELY.name());
    MODIFIED_DATE_ISSUE_MAP.put(DATE_INVALID, MODIFIED_DATE_INVALID.name());

    IDENTIFIED_DATE_ISSUE_MAP.put(DATE_INVALID, IDENTIFIED_DATE_INVALID.name());
    IDENTIFIED_DATE_ISSUE_MAP.put(DATE_UNLIKELY, IDENTIFIED_DATE_UNLIKELY.name());
  }

  /** {@link DwcTerm#eventDate} interpretation. */
  public static void interpretEventDate(ExtendedRecord er, TemporalRecord tr) {

    // Interpretation of the main dates
    String rawYear = extractValue(er, DwcTerm.year);
    String rawMonth = extractValue(er, DwcTerm.month);
    String rawDay = extractValue(er, DwcTerm.day);
    String rawEventDate = extractValue(er, DwcTerm.eventDate);

    // Call temporal parser
    ParsedTemporal temporalDates = TemporalParser.parse(rawYear, rawMonth, rawDay, rawEventDate);

    // Get all parsed values and set
    EventDate eventDate = new EventDate();

    temporalDates.getYearOpt().map(Year::getValue).ifPresent(tr::setYear);
    temporalDates.getMonthOpt().map(Month::getValue).ifPresent(tr::setMonth);
    temporalDates.getDayOpt().ifPresent(tr::setDay);
    temporalDates.getFromOpt().map(Temporal::toString).ifPresent(eventDate::setGte);
    temporalDates.getToOpt().map(Temporal::toString).ifPresent(eventDate::setLte);

    tr.setEventDate(eventDate);

    // Map issues to Interpretation
    temporalDates.getIssues().forEach(x ->
        Optional.ofNullable(EVENT_DATE_ISSUE_MAP.get(x)).ifPresent(tr.getIssues().getIssueList()::add)
    );
  }

  /** {@link DcTerm#modified} interpretation. */
  public static void interpretModifiedDate(ExtendedRecord er, TemporalRecord tr) {
    ParsedTemporal date = TemporalParser.parse(extractValue(er, DcTerm.modified));
    date.getFromOpt().map(Temporal::toString).ifPresent(tr::setModified);
    date.getIssues().forEach(x ->
        Optional.ofNullable(MODIFIED_DATE_ISSUE_MAP.get(x)).ifPresent(tr.getIssues().getIssueList()::add)
    );
  }

  /** {@link DwcTerm#dateIdentified} interpretation. */
  public static void interpretDateIdentified(ExtendedRecord er, TemporalRecord tr) {
    ParsedTemporal date = TemporalParser.parse(extractValue(er, DwcTerm.dateIdentified));
    date.getFromOpt().map(Temporal::toString).ifPresent(tr::setDateIdentified);
    date.getIssues().forEach(x ->
        Optional.ofNullable(IDENTIFIED_DATE_ISSUE_MAP.get(x)).ifPresent(tr.getIssues().getIssueList()::add)
    );
  }

  /** {@link DwcTerm#startDayOfYear} and {@link DwcTerm#endDayOfYear} interpretation. */
  public static void interpretDayOfYear(TemporalRecord tr) {
    Optional<LocalDate> year = Optional.ofNullable(tr.getYear()).map(y -> LocalDate.of(y, 1, 1));
    year.map(x -> x.with(TemporalAdjusters.lastDayOfYear())).ifPresent(x -> tr.setEndDayOfYear(x.getDayOfYear()));
    year.map(x -> x.with(TemporalAdjusters.firstDayOfYear())).ifPresent(x -> tr.setStartDayOfYear(x.getDayOfYear()));
  }
}
