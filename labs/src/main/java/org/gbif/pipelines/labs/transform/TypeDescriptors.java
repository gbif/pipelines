package org.gbif.pipelines.labs.transform;

import org.gbif.dwca.avro.ExtendedOccurrence;

import org.apache.beam.sdk.values.TypeDescriptor;

public class TypeDescriptors {

  private TypeDescriptors() {
    // Can't have an instance
  }

  public static TypeDescriptor<ExtendedOccurrence> extendedOccurrence() {
    return new TypeDescriptor<ExtendedOccurrence>() {};
  }

}
