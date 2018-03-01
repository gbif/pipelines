package org.gbif.pipelines.core;

import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TemporalRecord;
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

  public static TypeDescriptor<String> string() {
    return org.apache.beam.sdk.values.TypeDescriptors.strings();
  }

  public static TypeDescriptor<InterpretedExtendedRecord> interpretedExtendedRecord() {
    return new TypeDescriptor<InterpretedExtendedRecord>() {};
  }

  public static TypeDescriptor<IssueLineageRecord> issueLineageRecord() {
    return new TypeDescriptor<IssueLineageRecord>() {};
  }

  public static TypeDescriptor<TemporalRecord> temporalRecord() {
    return new TypeDescriptor<TemporalRecord>() {};
  }

  public static TypeDescriptor<OccurrenceIssue> occurrenceIssue() {
    return new TypeDescriptor<OccurrenceIssue>() {};
  }

}
