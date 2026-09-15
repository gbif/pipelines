package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Row-level value expression used by declarative target mappings. */
public interface ValueExpression extends Serializable {

  Set<FieldRef> requiredFields();

  static ValueExpression literal(Object value) {
    return new LiteralExpression(value);
  }

  static ValueExpression field(FieldRef field) {
    return new FieldExpression(field);
  }

  static ValueExpression concat(ValueExpression... expressions) {
    return new ConcatExpression(Arrays.asList(Arrays.copyOf(expressions, expressions.length)));
  }

  static ValueExpression firstNonBlank(ValueExpression... expressions) {
    return new FirstNonBlankExpression(
        Arrays.asList(Arrays.copyOf(expressions, expressions.length)));
  }

  record LiteralExpression(Object value) implements ValueExpression {
    @Override
    public Set<FieldRef> requiredFields() {
      return Set.of();
    }
  }

  record FieldExpression(FieldRef field) implements ValueExpression {
    public FieldExpression {
      Objects.requireNonNull(field, "field");
    }

    @Override
    public Set<FieldRef> requiredFields() {
      return Set.of(field);
    }
  }

  record ConcatExpression(List<ValueExpression> expressions) implements ValueExpression {
    public ConcatExpression {
      expressions = copyExpressions(expressions, "Concat");
    }

    @Override
    public Set<FieldRef> requiredFields() {
      return requiredFieldsOf(expressions);
    }
  }

  record FirstNonBlankExpression(List<ValueExpression> expressions) implements ValueExpression {
    public FirstNonBlankExpression {
      expressions = copyExpressions(expressions, "FirstNonBlank");
    }

    @Override
    public Set<FieldRef> requiredFields() {
      return requiredFieldsOf(expressions);
    }
  }

  private static List<ValueExpression> copyExpressions(
      List<ValueExpression> expressions, String name) {
    Objects.requireNonNull(expressions, "expressions");
    List<ValueExpression> copy = List.copyOf(expressions);
    if (copy.isEmpty()) {
      throw new IllegalArgumentException(name + " expression requires at least one child");
    }
    return copy;
  }

  private static Set<FieldRef> requiredFieldsOf(List<ValueExpression> expressions) {
    LinkedHashSet<FieldRef> fields = new LinkedHashSet<>();
    expressions.forEach(expression -> fields.addAll(expression.requiredFields()));
    return Set.copyOf(fields);
  }
}
