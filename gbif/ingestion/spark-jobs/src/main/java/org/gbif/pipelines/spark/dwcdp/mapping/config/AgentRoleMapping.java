package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder.coreFragment;
import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FilterExpression;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

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

  public record LinkedSpec(
      String fragmentName,
      String sourceResource,
      String parentResource,
      String sourceToParentViaColumn,
      String roleResource,
      String parentToRoleViaColumn,
      String role,
      String targetTerm,
      String agentValueField,
      ValueAggregation aggregation) {

    public LinkedSpec {
      Objects.requireNonNull(fragmentName, "fragmentName");
      Objects.requireNonNull(sourceResource, "sourceResource");
      Objects.requireNonNull(parentResource, "parentResource");
      Objects.requireNonNull(sourceToParentViaColumn, "sourceToParentViaColumn");
      Objects.requireNonNull(roleResource, "roleResource");
      Objects.requireNonNull(parentToRoleViaColumn, "parentToRoleViaColumn");
      Objects.requireNonNull(role, "role");
      Objects.requireNonNull(targetTerm, "targetTerm");
      Objects.requireNonNull(agentValueField, "agentValueField");
      Objects.requireNonNull(aggregation, "aggregation");
    }

    public static LinkedSpec orderedDistinctNames(
        String fragmentName,
        String sourceResource,
        String parentResource,
        String sourceToParentViaColumn,
        String roleResource,
        String parentToRoleViaColumn,
        String role,
        String targetTerm) {
      return new LinkedSpec(
          fragmentName,
          sourceResource,
          parentResource,
          sourceToParentViaColumn,
          roleResource,
          parentToRoleViaColumn,
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

    Paths paths = paths(graph, spec);
    return extensionFragment(spec.fragmentName(), rowType, paths.agent())
        .field(target(spec, paths))
        .build();
  }

  /** Builds a core enrichment from an AgentRole relationship. */
  public static CoreFragment core(SchemaGraph graph, Spec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(spec, "spec");

    Paths paths = paths(graph, spec);
    return coreFragment(spec.fragmentName(), paths.agent()).field(target(spec, paths)).build();
  }

  public static CoreFragment linkedCore(SchemaGraph graph, LinkedSpec spec) {
    LinkedPaths paths = linkedPaths(graph, spec);
    return coreFragment(spec.fragmentName(), paths.agent()).field(target(spec, paths)).build();
  }

  public static ExtensionFragment linkedExtension(
      SchemaGraph graph,
      String rowType,
      LinkedSpec spec,
      Optional<String> scopeKeyColumn,
      Optional<FieldRef> rowMatch) {
    LinkedPaths paths = linkedPaths(graph, spec);
    ExtensionFragmentBuilder builder =
        extensionFragment(spec.fragmentName(), rowType, paths.agent());
    scopeKeyColumn.ifPresent(builder::scopeKey);
    rowMatch.ifPresent(builder::rowMatch);
    return builder.field(target(spec, paths)).build();
  }

  private static TargetFieldMapping target(Spec spec, Paths paths) {
    return TargetFieldMapping.allOf(
            spec.targetTerm(), spec.aggregation(), paths.agent().field(spec.agentValueField()))
        .contributionIdentity(paths.role().field(AGENT_FK))
        .orderBy(paths.role().field(ROLE_ORDER_FIELD));
  }

  private static TargetFieldMapping target(LinkedSpec spec, LinkedPaths paths) {
    return TargetFieldMapping.allOf(
            spec.targetTerm(), spec.aggregation(), paths.agent().field(spec.agentValueField()))
        .contributionIdentity(paths.role().field(AGENT_FK))
        .orderBy(paths.role().field(ROLE_ORDER_FIELD));
  }

  private static LinkedPaths linkedPaths(SchemaGraph graph, LinkedSpec spec) {
    MappingPath source = MappingPath.root(graph, spec.sourceResource());
    MappingPath parent =
        source
            .join(spec.parentResource())
            .via(spec.sourceToParentViaColumn())
            .optional()
            .exactlyOne();
    MappingPath role =
        parent
            .join(spec.roleResource())
            .via(spec.parentToRoleViaColumn())
            .filter(FilterExpression.eq(ROLE_FIELD, spec.role()))
            .optional()
            .fanOut();
    MappingPath agent = role.join(AGENT_RESOURCE).via(AGENT_FK).optional().exactlyOne();
    return new LinkedPaths(role, agent);
  }

  private static Paths paths(SchemaGraph graph, Spec spec) {
    MappingPath parent = MappingPath.root(graph, spec.parentResource());
    MappingPath role =
        parent
            .join(spec.roleResource())
            .via(spec.parentViaColumn())
            .filter(FilterExpression.eq(ROLE_FIELD, spec.role()))
            .optional()
            .fanOut();
    MappingPath agent = role.join(AGENT_RESOURCE).via(AGENT_FK).optional().exactlyOne();
    return new Paths(role, agent);
  }

  private record Paths(MappingPath role, MappingPath agent) {}

  private record LinkedPaths(MappingPath role, MappingPath agent) {}
}
