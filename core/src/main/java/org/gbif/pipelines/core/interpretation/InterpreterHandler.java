package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.issue.Validation;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The {@link InterpreterHandler} providing the code to use to interpret record using the source,
 * the way how you can run you interpretation functions and consume result and issues
 *
 * <p>Example usage:
 *
 * <pre><code>
 * InterpreterHandler.of(extendedRecord, new LocationRecord())
 *    .withId(id)
 *    .using(interpretId())
 *    .using(interpretCountryAndCoordinates(wsConfig))
 *    .using(interpretContinent())
 *    .using(interpretWaterBody())
 *    .using(interpretStateProvince())
 *    .using(interpretMinimumElevationInMeters())
 *    .using(interpretMaximumElevationInMeters())
 *    .using(interpretMinimumDepthInMeters())
 *    .using(interpretMaximumDepthInMeters())
 *    .using(interpretMinimumDistanceAboveSurfaceInMeters())
 *    .using(interpretMaximumDistanceAboveSurfaceInMeters())
 *    .using(interpretCoordinatePrecision())
 *    .using(interpretCoordinateUncertaintyInMeters())
 *    .consumeData(d -> context.output(getDataTag(), KV.of(id, d)))
 *    .consumeIssue(i -> context.output(getIssueTag(), KV.of(id, i)));
 * </code></pre>
 *
 * @param <S> the type of the source element(Example: ExtendedRecord)
 * @param <V> the type of the output element(Example: LocationRecord)
 */
public class InterpreterHandler<S, V> {

  // Any source of data
  private final S source;
  // Container with the result value and issues
  private final Interpretation<V> value;
  // Identifier of the record for OccurrenceIssue
  private String id;

  /** Full constructor */
  private InterpreterHandler(S source, Interpretation<V> value) {
    this.source = source;
    this.value = value;
  }

  /** Full constructor, where value will be wrapped to {@link Interpretation} */
  private InterpreterHandler(S source, V value) {
    this.source = source;
    this.value = Interpretation.of(value);
  }

  /** Creates a hadler of a source and a value. */
  public static <S, V> InterpreterHandler<S, V> of(S source, Interpretation<V> value) {
    return new InterpreterHandler<>(source, value);
  }

  /** Creates a hadler of a source and a value. */
  public static <S, V> InterpreterHandler<S, V> of(S source, V value) {
    return new InterpreterHandler<>(source, value);
  }

  /** Identifier of the record for {@link OccurrenceIssue} */
  public InterpreterHandler<S, V> withId(String id) {
    this.id = id;
    return this;
  }

  /** Accepts interpretation function */
  public InterpreterHandler<S, V> using(BiConsumer<S, Interpretation<V>> function) {
    function.accept(source, value);
    return this;
  }

  /** Returns interpretation container */
  public Interpretation<V> getInterpretation() {
    return value;
  }

  /** Returns result value from interpretation container */
  public V getValue() {
    return value.getValue();
  }

  /** Converts {@link Interpretation.Trace} to {@link OccurrenceIssue} class */
  public OccurrenceIssue getOccurrenceIssue() {
    List<Validation> validations =
        value
            .getValidations()
            .stream()
            .map(
                x ->
                    Validation.newBuilder()
                        .setName(Optional.ofNullable(x.getFieldName()).orElse(""))
                        .setSeverity(x.getContext().toString())
                        .build())
            .collect(Collectors.toList());
    return OccurrenceIssue.newBuilder().setId(id).setIssues(validations).build();
  }

  /** Pushes the result of interpretation to consumer function */
  public InterpreterHandler<S, V> consumeData(Consumer<V> consumer) {
    consumer.accept(getValue());
    return this;
  }

  /** Pushes the issues of interpretation to consumer function */
  public InterpreterHandler<S, V> consumeIssue(Consumer<OccurrenceIssue> consumer) {
    if (!value.getValidations().isEmpty()) {
      consumer.accept(getOccurrenceIssue());
    }
    return this;
  }
}
