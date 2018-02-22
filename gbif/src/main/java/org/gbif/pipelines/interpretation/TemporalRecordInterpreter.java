package org.gbif.pipelines.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.interpretation.parsers.TemporalParser;
import org.gbif.pipelines.interpretation.parsers.temporal.ParsedTemporalDates;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface TemporalRecordInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  static TemporalRecordInterpreter interpretTemporal(TemporalRecord temporalRecord) {
    return (ExtendedRecord extendedRecord) -> {

      BiFunction<ExtendedRecord, DwcTerm, String> getValueFunc =
        (record, dwcTerm) -> Optional.ofNullable(record.getCoreTerms().get(dwcTerm.qualifiedName())).orElse("");

      // Get all raw values
      String rawYear = getValueFunc.apply(extendedRecord, DwcTerm.year);
      String rawMonth = getValueFunc.apply(extendedRecord, DwcTerm.month);
      String rawDay = getValueFunc.apply(extendedRecord, DwcTerm.day);
      String rawEventDate = getValueFunc.apply(extendedRecord, DwcTerm.eventDate);

      // Call main temporal parser
      ParsedTemporalDates temporalDates = TemporalParser.parse(rawYear, rawMonth, rawDay, rawEventDate);

      // Get all parsed values and set
      temporalDates.getYear().map(Year::getValue).ifPresent(temporalRecord::setYear);
      temporalDates.getMonth().map(Month::getValue).ifPresent(temporalRecord::setMonth);
      temporalDates.getDay().ifPresent(temporalRecord::setDay);

      //TODO: move delimiter definition to a more reusable  class
      //TODO: check with ES
      StringJoiner eventDay = new StringJoiner("/");
      temporalDates.getFrom().map(Temporal::toString).ifPresent(eventDay::add);
      temporalDates.getTo().map(Temporal::toString).ifPresent(eventDay::add);

      temporalRecord.setEventDate(eventDay.toString());

      // Map to Interpretation
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      interpretation.withValidation(DwcTerm.eventDate.name(), temporalDates.getIssueList());
      return interpretation;
    };
  }

}
