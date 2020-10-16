package org.gbif.pipelines.transforms.common;

import java.util.Arrays;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;

/**
 * Set of different predicate functions. Each function checks predicate and returns {@link
 * PCollection}, else returns empty {@link PCollection}
 */
@AllArgsConstructor(staticName = "create")
public class CheckTransforms<T> extends PTransform<PCollection<T>, PCollection<T>> {

  private final Class<T> clazz;
  private final boolean condition;

  /**
   * If the condition is FALSE returns empty collections, if you will you "write" data, it will
   * create an empty file, which is useful when you "read" files, cause Beam can throw an exception
   * if a file is absent
   */
  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return condition
        ? input
        : Create.empty(TypeDescriptor.of(clazz)).expand(PBegin.in(input.getPipeline()));
  }

  public static boolean checkRecordType(Set<String> types, InterpretationType... type) {
    boolean matchType = Arrays.stream(type).anyMatch(x -> types.contains(x.name()));
    boolean all = Arrays.stream(type).anyMatch(x -> types.contains(x.all()));
    return all || matchType;
  }
}
