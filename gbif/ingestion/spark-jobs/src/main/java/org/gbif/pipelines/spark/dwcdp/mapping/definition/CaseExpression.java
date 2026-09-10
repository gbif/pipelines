package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Ordered SQL-style CASE expression. The first matching branch wins. */
public final class CaseExpression implements ValueExpression {

  private final List<Branch> branches;
  private final ValueExpression otherwise;

  private CaseExpression(List<Branch> branches, ValueExpression otherwise) {
    this.branches = List.copyOf(branches);
    this.otherwise = Objects.requireNonNull(otherwise, "otherwise");
    if (branches.isEmpty()) {
      throw new IllegalArgumentException("Case expression requires at least one branch");
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public List<Branch> branches() {
    return branches;
  }

  public ValueExpression otherwise() {
    return otherwise;
  }

  @Override
  public Set<FieldRef> requiredFields() {
    LinkedHashSet<FieldRef> fields = new LinkedHashSet<>(otherwise.requiredFields());
    for (Branch branch : branches) {
      fields.addAll(branch.predicate().requiredFields());
      fields.addAll(branch.value().requiredFields());
    }
    return Set.copyOf(fields);
  }

  public record Branch(PredicateExpression predicate, ValueExpression value) {
    public Branch {
      Objects.requireNonNull(predicate, "predicate");
      Objects.requireNonNull(value, "value");
    }
  }

  public static final class Builder {
    private final List<Branch> branches = new ArrayList<>();

    private Builder() {}

    public Builder when(PredicateExpression predicate, ValueExpression value) {
      branches.add(new Branch(predicate, value));
      return this;
    }

    public Builder when(PredicateExpression predicate, Object value) {
      return when(predicate, ValueExpression.literal(value));
    }

    public Builder whenIn(ValueExpression expression, ValueExpression value, Object... candidates) {
      return when(PredicateExpression.in(expression, candidates), value);
    }

    public Builder whenIn(ValueExpression expression, Object value, Object... candidates) {
      return whenIn(expression, ValueExpression.literal(value), candidates);
    }

    public Builder whenAllIn(ValueExpression value, PredicateExpression.In... predicates) {
      return when(PredicateExpression.all(Arrays.copyOf(predicates, predicates.length)), value);
    }

    public Builder whenAllIn(Object value, PredicateExpression.In... predicates) {
      return whenAllIn(ValueExpression.literal(value), predicates);
    }

    public CaseExpression otherwise(ValueExpression value) {
      return new CaseExpression(branches, value);
    }

    public CaseExpression otherwise(Object value) {
      return otherwise(ValueExpression.literal(value));
    }
  }
}
