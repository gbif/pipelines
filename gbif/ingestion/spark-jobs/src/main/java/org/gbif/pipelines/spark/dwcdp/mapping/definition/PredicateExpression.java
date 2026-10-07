package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Row-level boolean expression used by declarative value expressions. */
public interface PredicateExpression extends Serializable {

  Set<FieldRef> requiredFields();

  static PredicateExpression equals(ValueExpression expression, Object value) {
    return new Equals(expression, ValueExpression.literal(value));
  }

  static PredicateExpression equals(ValueExpression left, ValueExpression right) {
    return new Equals(left, right);
  }

  static In in(ValueExpression expression, Object... values) {
    return new In(expression, Arrays.asList(Arrays.copyOf(values, values.length)));
  }

  static PredicateExpression all(PredicateExpression... predicates) {
    return new All(Arrays.asList(Arrays.copyOf(predicates, predicates.length)));
  }

  static PredicateExpression any(PredicateExpression... predicates) {
    return new Any(Arrays.asList(Arrays.copyOf(predicates, predicates.length)));
  }

  record Equals(ValueExpression left, ValueExpression right) implements PredicateExpression {
    public Equals {
      Objects.requireNonNull(left, "left");
      Objects.requireNonNull(right, "right");
    }

    @Override
    public Set<FieldRef> requiredFields() {
      return union(left.requiredFields(), right.requiredFields());
    }
  }

  record In(ValueExpression expression, List<?> values) implements PredicateExpression {
    public In {
      Objects.requireNonNull(expression, "expression");
      values = List.copyOf(values);
      if (values.isEmpty()) {
        throw new IllegalArgumentException("In predicate requires at least one candidate value");
      }
    }

    @Override
    public Set<FieldRef> requiredFields() {
      return expression.requiredFields();
    }
  }

  record All(List<PredicateExpression> predicates) implements PredicateExpression {
    public All {
      predicates = copyPredicates(predicates, "All");
    }

    @Override
    public Set<FieldRef> requiredFields() {
      return requiredFieldsOf(predicates);
    }
  }

  record Any(List<PredicateExpression> predicates) implements PredicateExpression {
    public Any {
      predicates = copyPredicates(predicates, "Any");
    }

    @Override
    public Set<FieldRef> requiredFields() {
      return requiredFieldsOf(predicates);
    }
  }

  private static List<PredicateExpression> copyPredicates(
      List<PredicateExpression> predicates, String name) {
    Objects.requireNonNull(predicates, "predicates");
    List<PredicateExpression> copy = List.copyOf(predicates);
    if (copy.isEmpty()) {
      throw new IllegalArgumentException(name + " predicate requires at least one child predicate");
    }
    return copy;
  }

  private static Set<FieldRef> requiredFieldsOf(List<PredicateExpression> predicates) {
    LinkedHashSet<FieldRef> fields = new LinkedHashSet<>();
    predicates.forEach(predicate -> fields.addAll(predicate.requiredFields()));
    return Set.copyOf(fields);
  }

  private static Set<FieldRef> union(Set<FieldRef> left, Set<FieldRef> right) {
    LinkedHashSet<FieldRef> fields = new LinkedHashSet<>(left);
    fields.addAll(right);
    return Set.copyOf(fields);
  }
}
