package org.gbif.pipelines.core.interpretation;

import org.gbif.pipelines.io.avro.issue.Issue;
import org.gbif.pipelines.io.avro.issue.IssueType;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A container object of interpretation result that can be combined with the result of other
 * interpretations.
 *
 * @param <T> type of context element use as an input for interpretation
 */
public class Interpretation<T> implements Serializable {

  private static final long serialVersionUID = -2685751511876257846L;

  // Element to be interpreted
  private final T value;
  // Stores the validations applied during an interpretation
  private final Set<Trace<IssueType>> validations;

  /** Creates a interpretation of a value. */
  public static <T> Interpretation<T> of(T value) {
    return new Interpretation<>(value, new HashSet<>());
  }

  /** Full constructor. */
  private Interpretation(T value, Set<Trace<IssueType>> validations) {
    this.value = value;
    this.validations = validations;
  }

  /** Adds a validation to the applied interpretation. */
  public Interpretation<T> withValidation(Trace<IssueType> validation) {
    validations.add(validation);
    return this;
  }

  /** Adds a validation to the applied interpretation. */
  public Interpretation<T> withValidation(String fieldName, Issue validation) {
    validations.add(Trace.of(fieldName, validation.getIssueType(), validation.getRemark()));
    return this;
  }

  public T getValue() {
    return value;
  }

  public Set<Trace<IssueType>> getValidations() {
    return validations;
  }

  /** Consumes all traces in the validation. */
  public void forEachValidation(Consumer<Trace<IssueType>> traceConsumer) {
    validations.forEach(traceConsumer);
  }

  /**
   * Container class for an element that needs to be tracked during an interpretation.
   *
   * @param <T> type of element to be tracked
   */
  public static class Trace<T> implements Serializable {

    private static final long serialVersionUID = -2440861649944996782L;

    private final String fieldName;
    // What this class is tracing
    private final T context;

    // Observation about a trace event
    private final String remark;

    /** Factory method to create a instance of trace object using a context element. */
    public static <U> Trace<U> of(String fieldName, U context) {
      return of(fieldName, context, null);
    }

    /** Factory method to create a instance of trace object using a context element. */
    public static <U> Trace<U> of(U context, String remark) {
      return of(null, context, remark);
    }

    /** Factory method to create a instance of trace object using a context element. */
    public static <U> Trace<U> of(U context) {
      return of(null, context, null);
    }

    /** Factory method to create a full instance of a trace object. */
    public static <U> Trace<U> of(String fieldName, U context, String remark) {
      return new Trace<>(fieldName, context, remark);
    }

    /** Creates an instance of traceable element. */
    private Trace(String fieldName, T context, String remark) {
      this.fieldName = fieldName;
      this.context = context;
      this.remark = remark;
    }

    /** field name of element being traced */
    public String getFieldName() {
      return fieldName;
    }

    /** @return the element being traced */
    public T getContext() {
      return context;
    }

    /** @return any comment or observation about the traced element */
    public String getRemark() {
      return remark;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Trace<?> trace = (Trace<?>) o;
      return Objects.equals(fieldName, trace.fieldName)
          && Objects.equals(context, trace.context)
          && Objects.equals(remark, trace.remark);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldName, context, remark);
    }
  }
}
