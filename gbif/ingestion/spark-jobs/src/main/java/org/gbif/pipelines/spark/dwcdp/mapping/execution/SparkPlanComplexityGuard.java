package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.Iterator;

/**
 * Fails before a Spark action when a generated logical plan is already pathologically large.
 *
 * <p>This deliberately inspects only the raw logical-plan and expression trees. It must not call
 * Catalyst canonicalization, constraint propagation or optimization APIs because those are exactly
 * the operations this guard is intended to protect from pathological generated plans.
 */
final class SparkPlanComplexityGuard {

  private static final Limits DEFAULT_LIMITS = new Limits(20_000, 250_000, 256, 100_000);

  private SparkPlanComplexityGuard() {}

  static void check(Dataset<?> dataset, String scope) {
    check(dataset, scope, DEFAULT_LIMITS);
  }

  static void check(Dataset<?> dataset, String scope, Limits limits) {
    Stats stats = new Stats(scope, limits);
    Set<LogicalPlan> visitedPlans =
        Collections.newSetFromMap(new IdentityHashMap<LogicalPlan, Boolean>());
    Set<Expression> visitedExpressions =
        Collections.newSetFromMap(new IdentityHashMap<Expression, Boolean>());
    walkPlan(dataset.queryExecution().logical(), stats, visitedPlans, visitedExpressions);
  }

  private static void walkPlan(
      LogicalPlan plan,
      Stats stats,
      Set<LogicalPlan> visitedPlans,
      Set<Expression> visitedExpressions) {
    if (!visitedPlans.add(plan)) {
      return;
    }
    stats.planNode();

    Iterator<Expression> expressions = plan.expressions().iterator();
    while (expressions.hasNext()) {
      walkExpression(expressions.next(), 1, stats, visitedExpressions);
    }

    Iterator<LogicalPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      walkPlan(children.next(), stats, visitedPlans, visitedExpressions);
    }
  }

  private static void walkExpression(
      Expression expression, int depth, Stats stats, Set<Expression> visitedExpressions) {
    if (!visitedExpressions.add(expression)) {
      return;
    }
    stats.expressionNode(expression, depth);

    Iterator<Expression> children = expression.children().iterator();
    while (children.hasNext()) {
      walkExpression(children.next(), depth + 1, stats, visitedExpressions);
    }
  }

  record Limits(
      int maxPlanNodes, int maxExpressionNodes, int maxExpressionDepth, int maxBooleanNodes) {}

  private static final class Stats {
    private final String scope;
    private final Limits limits;
    private int planNodes;
    private int expressionNodes;
    private int maxExpressionDepth;
    private int booleanNodes;

    private Stats(String scope, Limits limits) {
      this.scope = scope;
      this.limits = limits;
    }

    private void planNode() {
      planNodes++;
      validate();
    }

    private void expressionNode(Expression expression, int depth) {
      expressionNodes++;
      maxExpressionDepth = Math.max(maxExpressionDepth, depth);
      String type = expression.getClass().getSimpleName();
      if ("And".equals(type) || "Or".equals(type)) {
        booleanNodes++;
      }
      validate();
    }

    private void validate() {
      if (planNodes <= limits.maxPlanNodes()
          && expressionNodes <= limits.maxExpressionNodes()
          && maxExpressionDepth <= limits.maxExpressionDepth()
          && booleanNodes <= limits.maxBooleanNodes()) {
        return;
      }
      throw new IllegalStateException(
          "Generated Spark logical plan is excessively complex before execution for mapping scope "
              + scope
              + ". planNodes="
              + planNodes
              + ", expressionNodes="
              + expressionNodes
              + ", maxExpressionDepth="
              + maxExpressionDepth
              + ", booleanNodes="
              + booleanNodes
              + ". This usually indicates repeated fragment/path expansion or duplicated join/predicate "
              + "construction. Inspect the generated mapping before allowing Catalyst to canonicalize "
              + "or optimize this plan.");
    }
  }
}
