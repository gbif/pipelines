package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.core.RecordType;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.List;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import static org.gbif.pipelines.core.RecordType.ALL;
import static org.gbif.pipelines.core.RecordType.BASIC;
import static org.gbif.pipelines.core.RecordType.LOCATION;
import static org.gbif.pipelines.core.RecordType.METADATA;
import static org.gbif.pipelines.core.RecordType.MULTIMEDIA;
import static org.gbif.pipelines.core.RecordType.TAXONOMY;
import static org.gbif.pipelines.core.RecordType.TEMPORAL;

public class CheckTransforms<T> extends PTransform<PCollection<T>, PCollection<T>> {

  private final Class<T> clazz;
  private final boolean condition;

  private CheckTransforms(Class<T> clazz, boolean condition) {
    this.clazz = clazz;
    this.condition = condition;
  }

  public static CheckTransforms<String> metadata(List<String> types) {
    return create(String.class, checkRecordType(types, METADATA));
  }

  public static CheckTransforms<ExtendedRecord> basic(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, BASIC));
  }

  public static CheckTransforms<ExtendedRecord> temporal(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, TEMPORAL));
  }

  public static CheckTransforms<ExtendedRecord> multimedia(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, MULTIMEDIA));
  }

  public static CheckTransforms<ExtendedRecord> taxon(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, TAXONOMY));
  }

  public static CheckTransforms<ExtendedRecord> location(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, LOCATION));
  }

  public static <T> CheckTransforms<T> create(Class<T> clazz, boolean condition) {
    return new CheckTransforms<>(clazz, condition);
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    if (condition) {
      return input;
    }
    return Create.empty(TypeDescriptor.of(clazz)).expand(PBegin.in(input.getPipeline()));
  }

  private static boolean checkRecordType(List<String> types, RecordType type) {
    return types.contains(ALL.name()) || types.contains(type.name());
  }
}
