package org.gbif.pipelines.core.functions.descriptor;

import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.beam.sdk.values.TypeDescriptor;

public class CustomTypeDescriptors {

  private CustomTypeDescriptors() {
    // Can't have an instance
  }

  public static TypeDescriptor<TypedOccurrence> typedOccurrencies() {
    return new TypeDescriptor<TypedOccurrence>() {};
  }

  public static TypeDescriptor<UntypedOccurrence> untypedOccurrencies() {
    return new TypeDescriptor<UntypedOccurrence>() {};
  }

}
