package org.gbif.pipelines.spark.dwcdp.mapping.definition;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Value semantics for one target field. */
public interface TargetValue {

  List<FieldRef> sources();

  record Aggregated(
      TargetFieldMapping.SourceMode sourceMode,
      List<FieldRef> sources,
      ValueAggregation aggregation)
      implements TargetValue {

    public Aggregated {
      Objects.requireNonNull(sourceMode, "sourceMode");
      sources = List.copyOf(sources);
      Objects.requireNonNull(aggregation, "aggregation");
      if (sources.isEmpty()) {
        throw new IllegalArgumentException("Aggregated target value requires at least one source");
      }
    }
  }

  record Expression(ValueExpression expression) implements TargetValue {
    public Expression {
      Objects.requireNonNull(expression, "expression");
    }

    @Override
    public List<FieldRef> sources() {
      return expression.requiredFields().stream()
          .sorted(Comparator.comparing(FieldRef::toString))
          .toList();
    }
  }
}
