package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.io.Serializable;
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
}
