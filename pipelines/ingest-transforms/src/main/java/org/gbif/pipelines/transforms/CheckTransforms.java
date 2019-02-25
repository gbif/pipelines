package org.gbif.pipelines.transforms;

import java.util.List;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.ALL;

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

  public static boolean checkRecordType(List<String> types, RecordType type) {
    return types.contains(ALL.name()) || types.contains(type.name());
  }
}
