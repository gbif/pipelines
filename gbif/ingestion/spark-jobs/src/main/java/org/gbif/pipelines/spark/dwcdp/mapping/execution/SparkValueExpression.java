package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import org.apache.spark.sql.Column;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CaseExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.PredicateExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueExpression;

/** Generic Spark translation of declarative row-level value expressions. */
public final class SparkValueExpression {

  private SparkValueExpression() {}

  public static Column build(ValueExpression expression, Function<FieldRef, Column> fields) {
    Objects.requireNonNull(expression, "expression");
    Objects.requireNonNull(fields, "fields");

    if (expression instanceof ValueExpression.LiteralExpression literal) {
      return lit(literal.value());
    }
    if (expression instanceof ValueExpression.FieldExpression field) {
      return Objects.requireNonNull(fields.apply(field.field()), "Resolved field column");
    }
    if (expression instanceof CaseExpression caseExpression) {
      return buildCase(caseExpression, fields);
    }

    throw new UnsupportedOperationException(
        "Unsupported value expression: " + expression.getClass().getName());
  }

  private static Column buildCase(
      CaseExpression expression, Function<FieldRef, Column> fields) {
    Iterator<CaseExpression.Branch> branches = expression.branches().iterator();
    CaseExpression.Branch first = branches.next();
    Column result =
        when(build(first.predicate(), fields), build(first.value(), fields));

    while (branches.hasNext()) {
      CaseExpression.Branch branch = branches.next();
      result = result.when(build(branch.predicate(), fields), build(branch.value(), fields));
    }
    return result.otherwise(build(expression.otherwise(), fields));
  }

  private static Column build(
      PredicateExpression predicate, Function<FieldRef, Column> fields) {
    if (predicate instanceof PredicateExpression.Equals equals) {
      return build(equals.left(), fields).equalTo(build(equals.right(), fields));
    }
    if (predicate instanceof PredicateExpression.In in) {
      return build(in.expression(), fields).isin(in.values().toArray());
    }
    if (predicate instanceof PredicateExpression.All all) {
      Iterator<PredicateExpression> predicates = all.predicates().iterator();
      Column result = build(predicates.next(), fields);
      while (predicates.hasNext()) {
        result = result.and(build(predicates.next(), fields));
      }
      return result;
    }
    if (predicate instanceof PredicateExpression.Any any) {
      Iterator<PredicateExpression> predicates = any.predicates().iterator();
      Column result = build(predicates.next(), fields);
      while (predicates.hasNext()) {
        result = result.or(build(predicates.next(), fields));
      }
      return result;
    }

    throw new UnsupportedOperationException(
        "Unsupported predicate expression: " + predicate.getClass().getName());
  }
}
