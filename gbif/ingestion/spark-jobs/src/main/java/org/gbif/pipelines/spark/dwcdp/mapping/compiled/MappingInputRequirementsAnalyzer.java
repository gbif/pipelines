package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.Objects;
import org.gbif.pipelines.spark.dwcdp.mapping.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;

/** Derives the minimal safe physical input projection for a compiled mapping. */
public final class MappingInputRequirementsAnalyzer {
  private final SchemaGraph graph;

  public MappingInputRequirementsAnalyzer(SchemaGraph graph) {
    this.graph = Objects.requireNonNull(graph, "graph");
  }

  public MappingInputRequirements analyze(CompiledMapping mapping) {
    Objects.requireNonNull(mapping, "mapping");
    MappingInputRequirements.Builder out = new MappingInputRequirements.Builder();

    String core = mapping.coreSourceResource();
    out.use(core);
    addResourceIdentity(out, core);
    out.column(core, coreNaturalId(mapping));

    mapping.coreTargets().forEach(target -> addProducer(out, target));
    mapping.coreFragments().forEach(fragment -> addCoreFragment(out, core, fragment));
    mapping.coreTargetMerges().forEach(
        merge -> merge.producers().forEach(producer -> addProducer(out, producer)));

    mapping.extensions().forEach(
        extension -> {
          extension.targetMerges().forEach(
              merge -> merge.producers().forEach(producer -> addProducer(out, producer)));
          extension.fragments().forEach(fragment -> addExtensionFragment(out, core, fragment));
        });

    return out.build();
  }

  private void addCoreFragment(
      MappingInputRequirements.Builder out, String core, CompiledCoreFragment fragment) {
    out.use(fragment.sourceResource());
    addResourceIdentity(out, fragment.sourceResource());
    fragment.relations().forEach(relation -> addRelation(out, relation));
    fragment.targets().forEach(target -> addProducer(out, target));

    // Core-fragment materialization always bridges back through the physical core primary key.
    addResourceIdentity(out, core);
  }

  private void addExtensionFragment(
      MappingInputRequirements.Builder out, String core, CompiledFragment fragment) {
    out.use(fragment.sourceResource());
    addResourceIdentity(out, fragment.sourceResource());
    fragment.relations().forEach(relation -> addRelation(out, relation));
    addField(out, fragment.scopeKey());
    fragment.rowIdentity().ifPresent(field -> addField(out, field));
    fragment.rowMatch().ifPresent(field -> addField(out, field));
    fragment.targets().forEach(target -> addProducer(out, target));

    // Extension attachment is an executor-level relation rather than a target producer. Preserve
    // every schema-declared direct core<->root key that attachmentBridge may legally select.
    graph.relations(core, fragment.sourceResource())
        .forEach(relation -> addSchemaRelation(out, relation));
    addResourceIdentity(out, core);
  }

  private void addProducer(MappingInputRequirements.Builder out, CompiledTargetProducer producer) {
    producer.sources().forEach(source -> addField(out, source.field()));
    producer.contributionIdentity().ifPresent(source -> addField(out, source.field()));
    producer.orderBy().ifPresent(source -> addField(out, source.field()));
  }

  private void addField(MappingInputRequirements.Builder out, FieldRef field) {
    out.use(field.path().rootResource());
    for (SchemaRelation relation : field.path().relations()) {
      addSchemaRelation(out, relation);
    }
    out.column(field.path().currentResource(), field.column());
  }

  private void addRelation(
      MappingInputRequirements.Builder out, CompiledRelationStep relationStep) {
    addSchemaRelation(out, relationStep.relation());
    if (relationStep.filter().isPresent()) {
      // FilterExpression currently exposes an arbitrary Spark lambda. Until filters carry explicit
      // field dependencies, retaining the complete target resource is the only sound projection.
      out.allColumns(relationStep.relation().targetResource());
    }
  }

  private static void addSchemaRelation(
      MappingInputRequirements.Builder out, SchemaRelation relation) {
    out.column(relation.sourceResource(), relation.sourceColumn());
    out.column(relation.targetResource(), relation.targetColumn());
  }

  private void addResourceIdentity(MappingInputRequirements.Builder out, String resourceName) {
    graph.resource(resourceName)
        .ifPresent(
            resource -> {
              resource.primaryKey().ifPresent(column -> out.column(resourceName, column));
              resource.weakPrimaryKey().ifPresent(column -> out.column(resourceName, column));
            });
  }

  private static String coreNaturalId(CompiledMapping mapping) {
    return switch (mapping.coreType()) {
      case EVENT -> "eventID";
      case OCCURRENCE -> "occurrenceID";
    };
  }
}
