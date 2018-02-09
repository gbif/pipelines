package org.gbif.pipelines.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.interpretation.parsers.SimpleTypeParser;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.util.function.Function;

public interface TemporalInterpreter extends Function<ExtendedRecord,Interpretation<ExtendedRecord>> {


  /**
   * {@link DwcTerm#day} interpretation.
   */
  static ExtendedRecordInterpreter interpretDay(Event event) {
    return (ExtendedRecord extendedRecord) ->
      SimpleTypeParser.parseInt(extendedRecord, DwcTerm.day, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if(parseResult.isPresent()) {
          event.setDay(parseResult.get());
        } else {
          //TODO: we'll use this issue by now
          interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.RECORDED_DATE_INVALID));
        }
        return interpretation;
      });
  }


  /**
   * {@link DwcTerm#month} interpretation.
   */
  static ExtendedRecordInterpreter interpretMonth(Event event) {
    return (ExtendedRecord extendedRecord) ->
      SimpleTypeParser.parseInt(extendedRecord, DwcTerm.month, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if(parseResult.isPresent()) {
          event.setMonth(parseResult.get());
        } else {
          //TODO: we'll use this issue by now
          interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.RECORDED_DATE_INVALID));
        }
        return interpretation;
      });
  }


  /**
   * {@link DwcTerm#year} interpretation.
   */
  static ExtendedRecordInterpreter interpretYear(Event event) {
    return (ExtendedRecord extendedRecord) ->
      SimpleTypeParser.parseInt(extendedRecord, DwcTerm.year, parseResult -> {
        Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);
        if(parseResult.isPresent()) {
          event.setYear(parseResult.get());
        } else {
          //TODO: we'll use this issue by now
          interpretation.withValidation(Interpretation.Trace.of(OccurrenceIssue.RECORDED_DATE_INVALID));
        }
        return interpretation;
      });
  }

}
