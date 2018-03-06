package org.gbif.pipelines.labs.transform;

import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.beam.sdk.values.TypeDescriptor;

public class TypeDescriptors {

  private TypeDescriptors() {
    // Can't have an instance
  }

  public static TypeDescriptor<TypedOccurrence> typedOccurrence() {
    return new TypeDescriptor<TypedOccurrence>() {};
  }

  public static TypeDescriptor<UntypedOccurrence> untypedOccurrence() {
    return new TypeDescriptor<UntypedOccurrence>() {};
  }

}
