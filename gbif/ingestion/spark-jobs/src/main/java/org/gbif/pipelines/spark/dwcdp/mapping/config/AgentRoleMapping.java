package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder.coreFragment;
import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaPath;

/** Reusable mapping of DwC-DP {@code *-agent-role -> agent} relationships to scalar DwC-A terms. */
public final class AgentRoleMapping {

  private static final String AGENT_RESOURCE = "agent";
  private static final String AGENT_FK = "agent_fk";
  private static final String ROLE_FIELD = "agentRole";
  private static final String ROLE_ORDER_FIELD = "agentRoleOrder";
  private static final String DEFAULT_AGENT_VALUE_FIELD = "preferredAgentName";

  private AgentRoleMapping() {}

  /**
   * Describes one explicit role-to-target policy. The helper owns only the common AgentRole
   * topology; callers remain responsible for deciding which role contributes to which DwC-A term.
   */
  public record Spec(
      String fragmentName,
      String parentResource,
      String roleResource,
      String parentViaColumn,
      String role,
      String targetTerm,
      String agentValueField,
      ValueAggregation aggregation) {

    public Spec {
      Objects.requireNonNull(fragmentName, "fragmentName");
      Objects.requireNonNull(parentResource, "parentResource");
      Objects.requireNonNull(roleResource, "roleResource");
      Objects.requireNonNull(parentViaColumn, "parentViaColumn");
      Objects.requireNonNull(role, "role");
      Objects.requireNonNull(targetTerm, "targetTerm");
      Objects.requireNonNull(agentValueField, "agentValueField");
      Objects.requireNonNull(aggregation, "aggregation");
    }

    public static Spec orderedDistinctNames(
        String fragmentName,
        String parentResource,
        String roleResource,
        String parentViaColumn,
        String role,
        String targetTerm) {
      return new Spec(
          fragmentName,
          parentResource,
          roleResource,
          parentViaColumn,
          role,
          targetTerm,
          DEFAULT_AGENT_VALUE_FIELD,
          ValueAggregation.pipeDelimitedDistinct());
    }
  }

  /** Builds a parent-scoped extension enrichment from an AgentRole relationship. */
  public static ExtensionFragment extension(SchemaGraph graph, String rowType, Spec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(rowType, "rowType");
    Objects.requireNonNull(spec, "spec");

    Paths paths = resolvePaths(graph, spec);
    return extensionFragment(spec.fragmentName(), rowType, spec.parentResource())
        .join(spec.roleResource())
        .via(spec.parentViaColumn())
        .optional()
        .filter(FilterExpression.eq(ROLE_FIELD, spec.role()))
        .fanOut()
        .join(AGENT_RESOURCE)
        .via(AGENT_FK)
        .optional()
        .exactlyOne()
        .field(target(spec, paths))
        .build();
  }

  /** Builds a core enrichment from an AgentRole relationship. */
  public static CoreFragment core(SchemaGraph graph, Spec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(spec, "spec");

    Paths paths = resolvePaths(graph, spec);
    return coreFragment(spec.fragmentName(), spec.parentResource())
        .join(spec.roleResource())
        .via(spec.parentViaColumn())
        .optional()
        .filter(FilterExpression.eq(ROLE_FIELD, spec.role()))
        .fanOut()
        .join(AGENT_RESOURCE)
        .via(AGENT_FK)
        .optional()
        .exactlyOne()
        .field(target(spec, paths))
        .build();
  }

  private static TargetFieldMapping target(Spec spec, Paths paths) {
    return TargetFieldMapping.allOf(
            spec.targetTerm(), spec.aggregation(), paths.agent().field(spec.agentValueField()))
        .contributionIdentity(paths.role().field(AGENT_FK))
        .orderBy(paths.role().field(ROLE_ORDER_FIELD));
  }

  private static Paths resolvePaths(SchemaGraph graph, Spec spec) {
    SchemaPath parent = SchemaPath.root(spec.parentResource());
    SchemaPath role =
        parent.append(
            graph.resolve(
                spec.parentResource(), spec.roleResource(), spec.parentViaColumn(), null));
    SchemaPath agent =
        role.append(graph.resolve(spec.roleResource(), AGENT_RESOURCE, AGENT_FK, null));
    return new Paths(role, agent);
  }

  private record Paths(SchemaPath role, SchemaPath agent) {}
}
