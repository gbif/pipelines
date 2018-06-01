package org.gbif.pipelines.labs.functions;

import org.gbif.pipelines.io.avro.occurrence.TypedOccurrence;
import org.gbif.pipelines.io.avro.occurrence.UntypedOccurrence;
import org.gbif.pipelines.io.avro.occurrence.UntypedOccurrenceLowerCase;
import org.gbif.pipelines.io.avro.record.ExtendedRecord;
import org.gbif.pipelines.labs.functions.ws.gbif.species.NubSpeciesMatchFunction;

import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Provider of reusable functions. All functions implement serializable.
 */
public class FunctionFactory {

  private FunctionFactory() {
  }

  public static SerializableFunction<UntypedOccurrenceLowerCase, TypedOccurrence> interpretOccurrenceLowerCase() {
    return new InterpretOccurrenceLowerCase();
  }

  public static SerializableFunction<ExtendedRecord, UntypedOccurrence> untypedOccurrenceBuilder() {
    return new UntypedOccurrenceBuilder();
  }

  public static SerializableFunction<TypedOccurrence, TypedOccurrence> gbifSpeciesMatch() {
    return new NubSpeciesMatchFunction();
  }
}
