package org.gbif.pipelines.core.functions;

import org.gbif.pipelines.core.interpreter.temporal.ParsedDate;
import org.gbif.pipelines.core.interpreter.temporal.ParsedTemporalDate;
import org.gbif.pipelines.core.interpreter.TemporalInterpreterFunction;
import org.gbif.pipelines.io.avro.ParsedEventDateDemo;
import org.gbif.pipelines.io.avro.UntypedOccurrenceLowerCase;

public class InterpretOccurrenceEventDate implements SerializableFunction<UntypedOccurrenceLowerCase, ParsedEventDateDemo> {

  @Override
  public ParsedEventDateDemo apply(UntypedOccurrenceLowerCase source) {

    ParsedEventDateDemo target = new ParsedEventDateDemo();
    target.setOccurrenceId(source.getOccurrenceid());
    target.setRYear(source.getYear());
    target.setRMonth(source.getMonth());
    target.setRDay(source.getDay());
    target.setREvent("EVENT=" + source.getEventdate());

    ParsedTemporalDate parsedTemporalDate =
      TemporalInterpreterFunction.apply(source.getYear(), source.getMonth(), source.getDay(), source.getEventdate());
    ParsedDate first = parsedTemporalDate.getFrom();
    ParsedDate second = parsedTemporalDate.getTo();

    target.setCFirstEvent(first != null ? first.toString() : null);
    target.setCSecondEvent(second != null ? second.toString() : null);
    target.setCDuration("" + parsedTemporalDate.getDuration().getSeconds());

    return target;
  }
}