package org.gbif.pipelines.transforms;

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

/**
 * Set of different predicate functions. Each function checks predicate and returns {@link
 * PCollection}, else returns empty {@link PCollection}
 */
public class CheckTransforms<T> extends PTransform<PCollection<T>, PCollection<T>> {

  private final Class<T> clazz;
  private final boolean condition;

  private CheckTransforms(Class<T> clazz, boolean condition) {
    this.clazz = clazz;
    this.condition = condition;
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

  /**
   * Checks if list contains {@link RecordType#METADATA}, else returns empty {@link
   * PCollection<String>}
   */
  public static CheckTransforms<String> metadata(List<String> types) {
    return create(String.class, checkRecordType(types, METADATA));
  }

  /**
   * Checks if list contains {@link RecordType#BASIC}, else returns empty {@link
   * PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> basic(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, BASIC));
  }

  /**
   * Checks if list contains {@link RecordType#TEMPORAL}, else returns empty {@link
   * PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> temporal(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, TEMPORAL));
  }

  /**
   * Checks if list contains {@link RecordType#MULTIMEDIA}, else returns empty {@link
   * PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> multimedia(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, MULTIMEDIA));
  }

  /**
   * Checks if list contains {@link RecordType#TAXONOMY}, else returns empty {@link
   * PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> taxon(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, TAXONOMY));
  }

  /**
   * Checks if list contains {@link RecordType#LOCATION}, else returns empty {@link
   * PCollection<ExtendedRecord>}
   */
  public static CheckTransforms<ExtendedRecord> location(List<String> types) {
    return create(ExtendedRecord.class, checkRecordType(types, LOCATION));
  }

  private static boolean checkRecordType(List<String> types, RecordType type) {
    return types.contains(ALL.name()) || types.contains(type.name());
  }
}
