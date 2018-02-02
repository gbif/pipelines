package org.gbif.pipelines.core.functions;

import org.gbif.pipelines.core.interpreter.temporal.ParsedTemporalDate;
import org.gbif.pipelines.core.interpreter.TemporalInterpreterFunction;
import org.gbif.pipelines.io.avro.TypedOccurrence;

public class InterpretOccurrenceEventDate implements SerializableFunction<TypedOccurrence, TypedOccurrence> {

  @Override
  public TypedOccurrence apply(TypedOccurrence source) {

    ParsedTemporalDate temporalDate =
      TemporalInterpreterFunction.apply(source.getYear(), source.getMonth(), source.getDay(), source.getEventDate());

    String fromDate = temporalDate.getFrom() == null ? null : temporalDate.getFrom().toStringOrNull();

    return TypedOccurrence.newBuilder(source).setEventDate(fromDate).build();
  }
}