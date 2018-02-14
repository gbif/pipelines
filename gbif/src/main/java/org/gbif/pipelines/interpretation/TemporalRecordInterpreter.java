package org.gbif.pipelines.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.interpretation.parsers.temporal.ParsedTemporalDates;
import org.gbif.pipelines.interpretation.parsers.temporal.TemporalInterpreterFunction;
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

      BiFunction<ExtendedRecord, DwcTerm, String> getValueFunc =
        (record, dwcTerm) -> Optional.ofNullable(record.getCoreTerms().get(dwcTerm.qualifiedName())).orElse("").toString();

      String rawYear = getValueFunc.apply(extendedRecord, DwcTerm.year);
      String rawMonth = getValueFunc.apply(extendedRecord, DwcTerm.month);
      String rawDay = getValueFunc.apply(extendedRecord, DwcTerm.day);
      String rawEventDate = getValueFunc.apply(extendedRecord, DwcTerm.eventDate);

      ParsedTemporalDates temporalDates = TemporalInterpreterFunction.apply(rawYear, rawMonth, rawDay, rawEventDate);
      Integer year = temporalDates.getYear().map(Year::getValue).orElse(null);
      Integer month = temporalDates.getMonth().map(Month::getValue).orElse(null);
      Integer day = temporalDates.getDay().orElse(null);
      String from = temporalDates.getFrom().map(Temporal::toString).orElse(null);
      String to = temporalDates.getTo().map(Temporal::toString).orElse(null);
      //TODO: IMPROVE
      String eventDay = String.join("||", from, to);

      temporalRecord.setYear(year);
      temporalRecord.setMonth(month);
      temporalRecord.setDay(day);
      temporalRecord.setEventDate(eventDay);

      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
      // TODO: FIX
      //if (temporalDates.hasIssue()) {
      //  interpretation.withValidation(Interpretation.Trace.of(temporalDates.getIssue()));
      //}
      return interpretation;
    };
  }

}
