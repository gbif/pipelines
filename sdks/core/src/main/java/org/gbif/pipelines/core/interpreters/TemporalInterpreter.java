package org.gbif.pipelines.core.interpreters;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;
import org.gbif.pipelines.parsers.parsers.temporal.TemporalParser;

import java.time.LocalDate;
import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjusters;
import java.util.Optional;

import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;

/** Interprets the date related terms of a {@link ExtendedRecord}. */
public class TemporalInterpreter {

  private TemporalInterpreter() {}

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

    temporalDates.getYear().map(Year::getValue).ifPresent(tr::setYear);
    temporalDates.getMonth().map(Month::getValue).ifPresent(tr::setMonth);
    temporalDates.getDay().ifPresent(tr::setDay);
    temporalDates.getFrom().map(Temporal::toString).ifPresent(eventDate::setGte);
    temporalDates.getTo().map(Temporal::toString).ifPresent(eventDate::setLte);

    tr.setEventDate(eventDate);

    // Map issues to Interpretation
    tr.getIssues().setIssueList(temporalDates.getIssueList());
  }

  /** {@link DcTerm#modified} interpretation. */
  public static void interpretModifiedDate(ExtendedRecord er, TemporalRecord tr) {
    ParsedTemporal date = TemporalParser.parse(extractValue(er, DcTerm.modified));
    date.getFrom().map(Temporal::toString).ifPresent(tr::setModified);
    tr.getIssues().setIssueList(date.getIssueList());
  }

  /** {@link DwcTerm#dateIdentified} interpretation. */
  public static void interpretDateIdentified(ExtendedRecord er, TemporalRecord tr) {
    ParsedTemporal date = TemporalParser.parse(extractValue(er, DwcTerm.dateIdentified));
    date.getFrom().map(Temporal::toString).ifPresent(tr::setDateIdentified);
    tr.getIssues().setIssueList(date.getIssueList());
  }

  /** {@link DwcTerm#startDayOfYear} and {@link DwcTerm#endDayOfYear} interpretation. */
  public static void interpretDayOfYear(TemporalRecord tr) {
    Optional<LocalDate> year = Optional.ofNullable(tr.getYear()).map(y -> LocalDate.of(y, 1, 1));
    year.map(x -> x.with(TemporalAdjusters.lastDayOfYear()))
        .ifPresent(x -> tr.setEndDayOfYear(x.getDayOfYear()));
    year.map(x -> x.with(TemporalAdjusters.firstDayOfYear()))
        .ifPresent(x -> tr.setStartDayOfYear(x.getDayOfYear()));
  }
}
