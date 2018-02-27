package org.gbif.pipelines.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwca.avro.Event;
import org.gbif.pipelines.interpretation.column.InterpretationFactory;
import org.gbif.pipelines.interpretation.column.InterpretationResult;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

public interface TemporalInterpreter extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  /**
   * {@link DwcTerm#day} interpretation.
   */
  static ExtendedRecordInterpreter interpretDay(Event event) {
    return (ExtendedRecord extendedRecord) -> {
      InterpretationResult<Integer> result = InterpretationFactory.interpret(DwcTerm.day,
                                                                             extendedRecord.getCoreTerms()
                                                                               .get(DwcTerm.day.qualifiedName()));
      result.getResult().ifPresent(event::setDay);
      return result.asInterpretationOf(extendedRecord);
    };
  }

  /**
   * {@link DwcTerm#month} interpretation.
   */
  static ExtendedRecordInterpreter interpretMonth(Event event) {
    return (ExtendedRecord extendedRecord) -> {
      InterpretationResult<Integer> result = InterpretationFactory.interpret(DwcTerm.month,
                                                                             extendedRecord.getCoreTerms()
                                                                               .get(DwcTerm.month.qualifiedName()));
      result.getResult().ifPresent(event::setMonth);
      return result.asInterpretationOf(extendedRecord);
    };
  }

  /**
   * {@link DwcTerm#year} interpretation.
   */
  static ExtendedRecordInterpreter interpretYear(Event event) {
    return (ExtendedRecord extendedRecord) -> {
      InterpretationResult<Integer> result = InterpretationFactory.interpret(DwcTerm.year,
                                                                             extendedRecord.getCoreTerms()
                                                                               .get(DwcTerm.year.qualifiedName()));
      result.getResult().ifPresent(event::setYear);
      return result.asInterpretationOf(extendedRecord);
    };
  }

}
