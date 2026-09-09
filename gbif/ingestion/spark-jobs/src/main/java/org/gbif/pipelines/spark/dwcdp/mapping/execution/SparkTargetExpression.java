package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.filter;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.sort_array;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledTargetProducer;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;

/** Shared Spark interpretation of one compiled target producer. */
final class SparkTargetExpression {

  private SparkTargetExpression() {}

  static Column row(CompiledTargetProducer target, List<Column> sources) {
    if (target.sourceMode() == TargetFieldMapping.SourceMode.ONE_OF
        && target.aggregation() instanceof ValueAggregation.FirstNonNull) {
      return coalesce(sources.toArray(Column[]::new));
    }
    if (target.aggregation() instanceof ValueAggregation.ExactlyOne && sources.size() == 1) {
      return sources.get(0);
    }
    if (target.aggregation() instanceof ValueAggregation.FirstOrUrnFallback fallback) {
      if (sources.size() != 2) {
        throw new IllegalArgumentException(
            "FirstOrUrnFallback["
                + fallback.urn()
                + "] aggregation must have two sources for "
                + target.targetTerm());
      }
      Column naturalId = sources.get(0);
      return when(naturalId.isNotNull().and(length(trim(naturalId)).gt(0)), naturalId)
          .otherwise(concat(lit(fallback.urn()), sources.get(1)));
    }
    if (target.aggregation() instanceof ValueAggregation.LabeledOrFallback labeled) {
      if (sources.size() < 3) {
        throw new IllegalArgumentException(
            "LabeledOrFallback requires [label, name, fallback...] sources for "
                + target.targetTerm());
      }
      Column labeledValue =
          when(
                  sources.get(0).isNotNull().and(sources.get(1).isNotNull()),
                  concat(sources.get(0), lit(labeled.separator()), sources.get(1)))
              .otherwise(sources.get(2));
      if (sources.size() == 3) {
        return labeledValue;
      }
      List<Column> fallback = new ArrayList<>();
      fallback.add(labeledValue);
      fallback.addAll(sources.subList(3, sources.size()));
      return coalesce(fallback.toArray(Column[]::new));
    }
    if (target.aggregation() instanceof ValueAggregation.PreferredLabeledOrFallback labeled) {
      if (sources.size() < 4) {
        throw new IllegalArgumentException(
            "PreferredLabeledOrFallback requires [preferred, label, name, fallback...] sources for "
                + target.targetTerm());
      }
      Column labeledValue =
          when(
                  sources.get(1).isNotNull().and(sources.get(2).isNotNull()),
                  concat(sources.get(1), lit(labeled.separator()), sources.get(2)))
              .otherwise(sources.get(3));
      List<Column> values = new ArrayList<>();
      values.add(sources.get(0));
      values.add(labeledValue);
      values.addAll(sources.subList(4, sources.size()));
      return coalesce(values.toArray(Column[]::new));
    }

    throw new UnsupportedOperationException(
        "Unsupported row-level target aggregation for "
            + target.targetTerm()
            + ": "
            + target.aggregation());
  }

  static Column aggregate(
      CompiledTargetProducer target,
      List<Column> sources,
      Optional<Column> contributionIdentity,
      Optional<Column> orderBy) {
    if (target.sourceMode() == TargetFieldMapping.SourceMode.ONE_OF
        && target.aggregation() instanceof ValueAggregation.FirstNonNull) {
      return first(coalesce(sources.toArray(Column[]::new)), true);
    }

    if (target.aggregation() instanceof ValueAggregation.FirstOrUrnFallback fallback) {
      if (sources.size() != 2) {
        throw new IllegalArgumentException(
            "FirstOrUrnFallback["
                + fallback.urn()
                + "] aggregation must have two sources for "
                + target.targetTerm());
      }
      Column naturalId = sources.get(0);
      Column nonBlankNaturalId =
          when(naturalId.isNotNull().and(length(trim(naturalId)).gt(0)), naturalId);
      return coalesce(
          first(nonBlankNaturalId, true), concat(lit(fallback.urn()), first(sources.get(1), true)));
    }
    if (target.aggregation() instanceof ValueAggregation.Delimited delimited) {
      Column values;
      if (contributionIdentity.isPresent() || orderBy.isPresent()) {
        List<Column> contributionEntries = new ArrayList<>();
        for (Column source : sources) {
          List<Column> fields = new ArrayList<>();
          orderBy.ifPresent(order -> fields.add(order.as("order")));
          contributionIdentity.ifPresent(identity -> fields.add(identity.as("identity")));
          fields.add(source.cast("string").as("value"));
          contributionEntries.add(struct(fields.toArray(Column[]::new)));
        }

        Column contributions =
            flatten(collect_list(array(contributionEntries.toArray(Column[]::new))));
        if (contributionIdentity.isPresent()) {
          contributions = array_distinct(contributions);
        }
        if (orderBy.isPresent()) {
          contributions = sort_array(contributions);
        }

        values = transform(contributions, entry -> entry.getField("value"));
        values = filter(values, Column::isNotNull);
        if (delimited.distinct()) {
          values = array_distinct(values);
        }
        if (orderBy.isEmpty()) {
          values = sort_array(values);
        }
      } else {
        values = flatten(collect_list(array(sources.toArray(Column[]::new))));
        values = filter(values, Column::isNotNull);
        if (delimited.distinct()) {
          values = array_distinct(values);
        }
        values = sort_array(values);
      }

      return when(size(values).gt(0), concat_ws(delimited.delimiter(), values));
    }

    throw new UnsupportedOperationException(
        "Unsupported target aggregation for " + target.targetTerm() + ": " + target.aggregation());
  }
}
