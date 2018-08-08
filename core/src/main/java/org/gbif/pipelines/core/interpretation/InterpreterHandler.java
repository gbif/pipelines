package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.issue.Validation;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

// TODO: DOC!
public class InterpreterHandler<S, V> {

  private final S source;
  private final Interpretation<V> value;
  private String id;

  private InterpreterHandler(S source, V value) {
    this.source = source;
    this.value = Interpretation.of(value);
  }

  public static <S, V> InterpreterHandler<S, V> of(S source, V value) {
    return new InterpreterHandler<>(source, value);
  }

  public InterpreterHandler<S, V> withId(String id) {
    this.id = id;
    return this;
  }

  public InterpreterHandler<S, V> using(BiConsumer<S, Interpretation<V>> function) {
    function.accept(source, value);
    return this;
  }

  public Interpretation<V> getInterpretation() {
    return value;
  }

  public V getValue() {
    return value.getValue();
  }

  public OccurrenceIssue getOccurrenceIssue() {
    List<Validation> validations =
        value
            .getValidations()
            .stream()
            .map(
                x ->
                    Validation.newBuilder()
                        .setName(x.getFieldName())
                        .setSeverity(x.getContext().toString())
                        .build())
            .collect(Collectors.toList());
    return OccurrenceIssue.newBuilder().setId(id).setIssues(validations).build();
  }

  public InterpreterHandler<S, V> consumeData(Consumer<V> consumer) {
    consumer.accept(getValue());
    return this;
  }

  public InterpreterHandler<S, V> consumeIssue(Consumer<OccurrenceIssue> consumer) {
    if (!value.getValidations().isEmpty()) {
      consumer.accept(getOccurrenceIssue());
    }
    return this;
  }
}
