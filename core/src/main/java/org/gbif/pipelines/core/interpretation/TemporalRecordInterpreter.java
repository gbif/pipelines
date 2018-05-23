package org.gbif.pipelines.core.interpretation;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.pipelines.core.interpretation.Interpretation.Trace;
import org.gbif.pipelines.core.parsers.TemporalParser;
import org.gbif.pipelines.core.parsers.temporal.ParsedTemporalDates;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface TemporalRecordInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  static TemporalRecordInterpreter interpretTemporal(TemporalRecord temporalRecord) {
    return (ExtendedRecord extendedRecord) -> {

      BiFunction<ExtendedRecord, Term, String> getValueFunc =
        (record, dwcTerm) -> Optional.ofNullable(record.getCoreTerms().get(dwcTerm.qualifiedName())).orElse("");

      // Interpretation of the main dates
      String rawYear = getValueFunc.apply(extendedRecord, DwcTerm.year);
      String rawMonth = getValueFunc.apply(extendedRecord, DwcTerm.month);
      String rawDay = getValueFunc.apply(extendedRecord, DwcTerm.day);
      String rawEventDate = getValueFunc.apply(extendedRecord, DwcTerm.eventDate);

      // Call temporal parser
      ParsedTemporalDates temporalDates = TemporalParser.parse(rawYear, rawMonth, rawDay, rawEventDate);

      // Get all parsed values and set
      EventDate eventDate = new EventDate();

      temporalDates.getYear().map(Year::getValue).ifPresent(temporalRecord::setYear);
      temporalDates.getMonth().map(Month::getValue).ifPresent(temporalRecord::setMonth);
      temporalDates.getDay().ifPresent(temporalRecord::setDay);
      temporalDates.getFrom().map(Temporal::toString).ifPresent(eventDate::setGte);
      temporalDates.getTo().map(Temporal::toString).ifPresent(eventDate::setLte);

      temporalRecord.setEventDate(eventDate);

      // Create interpretation
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

      // Map issues to Interpretation
      temporalDates.getIssueList()
        .forEach(issue -> interpretation.withValidation(Trace.of(DwcTerm.eventDate.name(), issue)));

      // Interpretation of the modified date
      ParsedTemporalDates temporalModifiedDate =
        TemporalParser.parse(getValueFunc.apply(extendedRecord, DcTerm.modified));
      temporalModifiedDate.getFrom().map(Temporal::toString).ifPresent(temporalRecord::setModified);
      temporalModifiedDate.getIssueList()
        .forEach(issue -> interpretation.withValidation(Trace.of(DcTerm.modified.name(), issue)));

      // Interpretation of the dateIdentified
      ParsedTemporalDates temporalDateIdentified =
        TemporalParser.parse(getValueFunc.apply(extendedRecord, DwcTerm.dateIdentified));
      temporalDateIdentified.getFrom().map(Temporal::toString).ifPresent(temporalRecord::setDateIdentified);
      temporalDateIdentified.getIssueList()
        .forEach(issue -> interpretation.withValidation(Trace.of(DwcTerm.dateIdentified.name(), issue)));

      return interpretation;
    };
  }

}
