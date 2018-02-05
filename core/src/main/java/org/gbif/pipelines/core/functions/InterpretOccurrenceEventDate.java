package org.gbif.pipelines.core.functions;

import org.gbif.pipelines.core.interpreter.temporal.ParsedDate;
import org.gbif.pipelines.core.interpreter.temporal.ParsedTemporalDate;
import org.gbif.pipelines.core.interpreter.TemporalInterpreterFunction;
import org.gbif.pipelines.io.avro.TypedOccurrence;

import java.util.Optional;

public class InterpretOccurrenceEventDate implements SerializableFunction<TypedOccurrence, TypedOccurrence> {

  @Override
  public TypedOccurrence apply(TypedOccurrence source) {

    ParsedTemporalDate temporalDate =
      TemporalInterpreterFunction.apply(source.getYear(), source.getMonth(), source.getDay(), source.getEventDate());

    Optional<ParsedDate> from = temporalDate.getFrom();
    String fromDate = from.isPresent() ? from.get().toStringOrNull() : null;

    return TypedOccurrence.newBuilder(source).setEventDate(fromDate).build();
  }
}