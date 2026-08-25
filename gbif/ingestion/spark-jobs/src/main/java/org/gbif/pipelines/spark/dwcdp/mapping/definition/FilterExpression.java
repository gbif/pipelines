package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.FieldColumns;

/**
 * Spark SQL predicate used by a mapping relation.
 *
 * <p>Built-in expressions expose their physical field dependencies so input projection and path
 * pruning can remain precise. A raw lambda remains available as the escape hatch; because its
 * dependencies are opaque it conservatively requires all columns of the relation target resource.
 */
@FunctionalInterface
public interface FilterExpression extends Serializable {

  Column build(FieldColumns columns);

  default boolean isPresent() {
    return true;
  }

  /** Physical columns on the relation target resource referenced by this predicate. */
  default Set<String> requiredColumns() {
    return Set.of();
  }

  /** True for opaque/raw predicates whose dependencies cannot be inspected safely. */
  default boolean requiresAllColumns() {
    return isPresent() && requiredColumns().isEmpty();
  }

  static FilterExpression none() {
    return new Declared(Set.of(), false, columns -> lit(true)) {
      @Override
      public boolean isPresent() {
        return false;
      }
    };
  }

  static FilterExpression eq(String field, Object value) {
    return declared(Set.of(field), columns -> columns.col(field).equalTo(value));
  }

  static FilterExpression isNull(String field) {
    return declared(Set.of(field), columns -> columns.col(field).isNull());
  }

  static FilterExpression isNotNull(String field) {
    return declared(Set.of(field), columns -> columns.col(field).isNotNull());
  }

  static FilterExpression in(String field, Object... values) {
    Object[] copy = Arrays.copyOf(values, values.length);
    return declared(Set.of(field), columns -> columns.col(field).isin(copy));
  }

  /** Like {@link #in}, but tolerates a schema-declared column missing from the physical table. */
  static FilterExpression optionalIn(String field, Object... values) {
    Object[] copy = Arrays.copyOf(values, values.length);
    return declared(Set.of(field), columns -> columns.colOrNull(field).isin(copy));
  }

  static FilterExpression and(FilterExpression left, FilterExpression right) {
    return combine(left, right, true);
  }

  static FilterExpression or(FilterExpression left, FilterExpression right) {
    return combine(left, right, false);
  }

  /** Explicitly marks a predicate as opaque to dependency analysis. */
  static FilterExpression raw(FilterExpression expression) {
    Objects.requireNonNull(expression, "expression");
    return expression::build;
  }

  private static FilterExpression combine(
      FilterExpression left, FilterExpression right, boolean conjunction) {
    Objects.requireNonNull(left, "left");
    Objects.requireNonNull(right, "right");
    if (left.requiresAllColumns() || right.requiresAllColumns()) {
      return raw(
          columns ->
              conjunction
                  ? left.build(columns).and(right.build(columns))
                  : left.build(columns).or(right.build(columns)));
    }
    LinkedHashSet<String> fields = new LinkedHashSet<>(left.requiredColumns());
    fields.addAll(right.requiredColumns());
    return declared(
        fields,
        columns ->
            conjunction
                ? left.build(columns).and(right.build(columns))
                : left.build(columns).or(right.build(columns)));
  }

  private static FilterExpression declared(Set<String> fields, FilterExpression expression) {
    Objects.requireNonNull(expression, "expression");
    return new Declared(fields, true, expression);
  }

  class Declared implements FilterExpression {
    private final Set<String> fields;
    private final boolean present;
    private final FilterExpression delegate;

    Declared(Set<String> fields, boolean present, FilterExpression delegate) {
      this.fields = Set.copyOf(fields);
      this.present = present;
      this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public Column build(FieldColumns columns) {
      return delegate.build(columns);
    }

    @Override
    public boolean isPresent() {
      return present;
    }

    @Override
    public Set<String> requiredColumns() {
      return fields;
    }

    @Override
    public boolean requiresAllColumns() {
      return false;
    }
  }
}
