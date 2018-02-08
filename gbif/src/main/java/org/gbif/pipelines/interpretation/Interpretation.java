package org.gbif.pipelines.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A container object of interpretation result that can be combined with the result of other interpretations.
 * @param <T> type of context element use as an input for interpretation
 */
public class Interpretation<T> implements Serializable {

  /**
   * Container class for an element that needs to be tracked during an interpretation.
   * @param <T> type of element to be tracked
   */
  public static class Trace<T> implements Serializable {

    //What this class is tracing
    private final T context;

    //Observation about a trace event
    private final String remark;

    /**
     *  Creates an instance of traceable element.
     */
    private Trace(T context, String remark) {
      this.context = context;
      this.remark = remark;
    }

    /**
     * Factory method to create a instance of trace object using a context element.
     */
    public static <U> Trace<U> of(U context) {
      return new Trace<>(context, null);
    }

    /**
     * Factory method to create a full instance of a trace object.
     */
    public static <U> Trace<U> of(U context, String remark) {
      return new Trace<>(context, remark);
    }

    /**
     *
     * @return the element being traced
     */
    public T getContext() {
      return context;
    }

    /**
     *
     * @return any comment or observation about the traced element
     */
    public String getRemark() {
      return remark;
    }
  }

  //Element to be interpreted
  private final T value;

  //Stores the transformations and operations applied during an interpretation
  private final List<Trace<String>> lineage;

  //Stores the validations applied during an interpretation
  private final List<Trace<OccurrenceIssue>> validations;

  /**
   * Full constructor.
   */
  private Interpretation(T value,  List<Trace<OccurrenceIssue>> validations, List<Trace<String>> lineage) {
    this.value = value;
    this.validations = validations;
    this.lineage = lineage;
  }

  /**
   * Creates a interpretation of a value.
   */
  public static <U> Interpretation<U> of(U value) {
    return new Interpretation<>(value, Collections.emptyList(), Collections.emptyList());
  }

  /**
   * Adds a validation to the applied interpretation.
   */
  public Interpretation<T> withValidation(Trace<OccurrenceIssue> validation) {
    validations.add(validation);
    return this;
  }


  /**
   * Adds a lineage trace to the interpretation operation.
   */
  public Interpretation<T> withLineage(Trace<String> lineage) {
    this.lineage.add(lineage);
    return this;
  }

  public <U> Interpretation<U> using(Function<? super T, Interpretation<U>> mapper) {
    Interpretation<U> interpretation = mapper.apply(value);

    List<Trace<String>> newLineage = new ArrayList<>(lineage);
    newLineage.addAll(interpretation.lineage);

    List<Trace<OccurrenceIssue>> newValidations = new ArrayList<>(validations);
    newValidations.addAll(interpretation.validations);

    return new Interpretation<>(interpretation.value, validations, newLineage);
  }

  /**
   * Consumes all traces in the validation.
   */
  public void forEachValidation(Consumer<Trace<OccurrenceIssue>> traceConsumer) {
    validations.forEach(traceConsumer);
  }

  /**
   * Consumes all traces in the lineage.
   */
  public void forEachLineage(Consumer<Trace<String>> traceConsumer) {
    lineage.forEach(traceConsumer);
  }

}
