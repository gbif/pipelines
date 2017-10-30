package org.gbif.pipelines.core.functions;

import org.gbif.dwca.record.StarRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.util.function.Function;

/**
 * Provider of reusable functions.
 * All functions implement serializable.
 */
public class Functions {

  public static SerializableFunction<UntypedOccurrence, TypedOccurrence> interpretOccurrence() {
    return new InterpretOccurrence();
  }

  public static SerializableFunction<StarRecord, ExtendedRecord> extendedRecordBuilder() {
    return new ExtendedRecordBuilder();
  }

  public static SerializableFunction<ExtendedRecord, UntypedOccurrence> untypedOccurrenceBuilder() {
    return new UntypedOccurrenceBuilder();
  }

  public static SerializableFunction<TypedOccurrence, TypedOccurrence> gbifSpeciesMatch(String baseUrl) {
    return new NubSpeciesMatch(baseUrl);
  }
}
