package org.gbif.pipelines.spark.dwcdp.mapping.config;

import static org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragmentBuilder.coreFragment;
import static org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder.extensionFragment;

import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionFragmentBuilder;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPath;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;

/** Reusable explicit text + agent-ID resolution for DwC-DP agent-valued fields. */
public final class AgentMapping {

  private static final String AGENT_RESOURCE = "agent";
  private static final String AGENT_NAME = "preferredAgentName";

  private AgentMapping() {}

  public record Spec(
      String fragmentName,
      String sourceResource,
      String idColumn,
      String valueColumn,
      String targetTerm) {

    public Spec {
      Objects.requireNonNull(fragmentName, "fragmentName");
      Objects.requireNonNull(sourceResource, "sourceResource");
      Objects.requireNonNull(idColumn, "idColumn");
      Objects.requireNonNull(valueColumn, "valueColumn");
      Objects.requireNonNull(targetTerm, "targetTerm");
    }
  }

  public record LinkedSpec(
      String fragmentName,
      String sourceResource,
      String linkedResource,
      String linkedViaColumn,
      String idColumn,
      String valueColumn,
      String targetTerm) {

    public LinkedSpec {
      Objects.requireNonNull(fragmentName, "fragmentName");
      Objects.requireNonNull(sourceResource, "sourceResource");
      Objects.requireNonNull(linkedResource, "linkedResource");
      Objects.requireNonNull(linkedViaColumn, "linkedViaColumn");
      Objects.requireNonNull(idColumn, "idColumn");
      Objects.requireNonNull(valueColumn, "valueColumn");
      Objects.requireNonNull(targetTerm, "targetTerm");
    }
  }
  /** Resolves a core resource's weak/natural agent ID while preserving explicit publisher text. */
  public static CoreFragment core(SchemaGraph graph, Spec spec) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(spec, "spec");

    MappingPath source = MappingPath.root(graph, spec.sourceResource());
    MappingPath agent = source.join(AGENT_RESOURCE).via(spec.idColumn()).optional().fanOut();

    return coreFragment(spec.fragmentName(), agent).field(target(spec, source, agent)).build();
  }

  /**
   * Resolves an extension resource's weak/natural agent ID while preserving explicit publisher
   * text.
   */
  public static ExtensionFragment extension(
      SchemaGraph graph,
      String rowType,
      Spec spec,
      Optional<String> scopeKeyColumn,
      Optional<FieldRef> rowMatch) {
    Objects.requireNonNull(graph, "graph");
    Objects.requireNonNull(rowType, "rowType");
    Objects.requireNonNull(spec, "spec");
    Objects.requireNonNull(scopeKeyColumn, "scopeKeyColumn");
    Objects.requireNonNull(rowMatch, "rowMatch");

    MappingPath source = MappingPath.root(graph, spec.sourceResource());
    MappingPath agent = source.join(AGENT_RESOURCE).via(spec.idColumn()).optional().fanOut();

    ExtensionFragmentBuilder builder = extensionFragment(spec.fragmentName(), rowType, agent);
    scopeKeyColumn.ifPresent(builder::scopeKey);
    rowMatch.ifPresent(builder::rowMatch);

    return builder.field(target(spec, source, agent)).build();
  }

  public static ExtensionFragment extension(SchemaGraph graph, String rowType, Spec spec) {
    return extension(graph, rowType, spec, Optional.empty(), Optional.empty());
  }

  public static CoreFragment linkedCore(SchemaGraph graph, LinkedSpec spec) {
    MappingPath source = MappingPath.root(graph, spec.sourceResource());
    MappingPath linked =
        source.join(spec.linkedResource()).via(spec.linkedViaColumn()).optional().exactlyOne();
    MappingPath agent = linked.join(AGENT_RESOURCE).via(spec.idColumn()).optional().fanOut();

    return coreFragment(spec.fragmentName(), agent)
        .field(target(spec.targetTerm(), linked.field(spec.valueColumn()), agent.field(AGENT_NAME)))
        .build();
  }

  public static ExtensionFragment linkedExtension(
      SchemaGraph graph,
      String rowType,
      LinkedSpec spec,
      Optional<String> scopeKeyColumn,
      Optional<FieldRef> rowMatch) {
    MappingPath source = MappingPath.root(graph, spec.sourceResource());
    MappingPath linked =
        source.join(spec.linkedResource()).via(spec.linkedViaColumn()).optional().exactlyOne();
    MappingPath agent = linked.join(AGENT_RESOURCE).via(spec.idColumn()).optional().fanOut();

    ExtensionFragmentBuilder builder = extensionFragment(spec.fragmentName(), rowType, agent);
    scopeKeyColumn.ifPresent(builder::scopeKey);
    rowMatch.ifPresent(builder::rowMatch);

    return builder
        .field(target(spec.targetTerm(), linked.field(spec.valueColumn()), agent.field(AGENT_NAME)))
        .build();
  }

  private static TargetFieldMapping target(Spec spec, MappingPath source, MappingPath agent) {
    return target(spec.targetTerm(), source.field(spec.valueColumn()), agent.field(AGENT_NAME));
  }

  private static TargetFieldMapping target(String targetTerm, FieldRef value, FieldRef agentName) {
    return TargetFieldMapping.oneOf(targetTerm, ValueAggregation.firstNonNull(), value, agentName);
  }
}
