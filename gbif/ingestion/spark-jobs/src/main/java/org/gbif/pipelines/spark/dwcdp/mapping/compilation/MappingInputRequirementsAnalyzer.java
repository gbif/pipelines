package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.Objects;
import java.util.Optional;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;

/** Derives the minimal safe physical input projection for a compiled mapping. */
public final class MappingInputRequirementsAnalyzer {
  private final SchemaGraph graph;
  private final Optional<MappingDatasetScope> datasetScope;

  public MappingInputRequirementsAnalyzer(SchemaGraph graph) {
    this(graph, Optional.empty());
  }

  public MappingInputRequirementsAnalyzer(SchemaGraph graph, MappingDatasetScope datasetScope) {
    this(graph, Optional.of(Objects.requireNonNull(datasetScope, "datasetScope")));
  }

  private MappingInputRequirementsAnalyzer(
      SchemaGraph graph, Optional<MappingDatasetScope> datasetScope) {
    this.graph = Objects.requireNonNull(graph, "graph");
    this.datasetScope = Objects.requireNonNull(datasetScope, "datasetScope");
  }

  public MappingInputRequirements analyze(CompiledMapping mapping) {
    Objects.requireNonNull(mapping, "mapping");
    MappingInputRequirements.Builder out = new MappingInputRequirements.Builder();

    String core = mapping.coreSourceResource();
    use(out, core);
    addResourceIdentity(out, core);
    mapping.coreIdentity().ifPresent(identity -> addProducer(out, identity));

    mapping.coreTargets().forEach(target -> addProducer(out, target));
    mapping.coreFragments().forEach(fragment -> addCoreFragment(out, core, fragment));
    mapping
        .coreTargetMerges()
        .forEach(merge -> merge.producers().forEach(producer -> addProducer(out, producer)));

    mapping
        .extensions()
        .forEach(
            extension -> {
              extension
                  .targetMerges()
                  .forEach(
                      merge -> merge.producers().forEach(producer -> addProducer(out, producer)));
              extension.fragments().forEach(fragment -> addExtensionFragment(out, core, fragment));
            });

    return out.build();
  }

  private void addCoreFragment(
      MappingInputRequirements.Builder out, String core, CompiledCoreFragment fragment) {
    use(out, fragment.sourceResource());
    addResourceIdentity(out, fragment.sourceResource());
    fragment.relations().forEach(relation -> addRelation(out, relation));
    fragment.targets().forEach(target -> addProducer(out, target));

    // Core-fragment materialization always bridges back through the physical core primary key.
    addResourceIdentity(out, core);
  }

  private void addExtensionFragment(
      MappingInputRequirements.Builder out, String core, CompiledFragment fragment) {
    use(out, fragment.sourceResource());
    addResourceIdentity(out, fragment.sourceResource());
    fragment.relations().forEach(relation -> addRelation(out, relation));
    addField(out, fragment.scopeKey());
    fragment.rowIdentity().ifPresent(field -> addField(out, field));
    fragment.rowMatch().ifPresent(field -> addField(out, field));
    fragment.targets().forEach(target -> addProducer(out, target));

    // Extension attachment is an executor-level relation rather than a target producer. Preserve
    // every schema-declared direct core<->root key that attachmentBridge may legally select.
    graph
        .relations(core, fragment.sourceResource())
        .forEach(relation -> addSchemaRelation(out, relation));
    addResourceIdentity(out, core);
  }

  private void addProducer(MappingInputRequirements.Builder out, CompiledTargetProducer producer) {
    producer.sources().forEach(source -> addField(out, source.field()));
    producer.contributionIdentity().ifPresent(source -> addField(out, source.field()));
    producer.orderBy().ifPresent(source -> addField(out, source.field()));
  }

  private void addField(MappingInputRequirements.Builder out, FieldRef field) {
    if (!supports(field)) {
      return;
    }
    use(out, field.path().rootResource());
    for (SchemaRelation relation : field.path().relations()) {
      addSchemaRelation(out, relation);
    }
    column(out, field.path().currentResource(), field.column());
  }

  private void addRelation(
      MappingInputRequirements.Builder out, CompiledRelationStep relationStep) {
    if (!supports(relationStep)) {
      return;
    }
    addSchemaRelation(out, relationStep.relation());
    relationStep
        .cardinalityStrategy()
        .ifPresent(
            strategy -> {
              if (strategy
                  instanceof
                  org.gbif.pipelines.spark.dwcdp.mapping.definition.CardinalityStrategy.Select
                  select) {
                column(out, relationStep.relation().targetResource(), select.selector());
              }
            });
    if (relationStep.filter().isPresent()) {
      String targetResource = relationStep.relation().targetResource();
      if (relationStep.filter().requiresAllColumns()) {
        allColumns(out, targetResource);
      } else {
        relationStep
            .filter()
            .requiredColumns()
            .forEach(field -> column(out, targetResource, field));
      }
    }
  }

  private void addSchemaRelation(MappingInputRequirements.Builder out, SchemaRelation relation) {
    if (!hasResource(relation.sourceResource()) || !hasResource(relation.targetResource())) {
      return;
    }
    column(out, relation.sourceResource(), relation.sourceColumn());
    column(out, relation.targetResource(), relation.targetColumn());
  }

  private void addResourceIdentity(MappingInputRequirements.Builder out, String resourceName) {
    if (!hasResource(resourceName)) {
      return;
    }
    graph
        .resource(resourceName)
        .ifPresent(
            resource -> {
              resource.primaryKey().ifPresent(column -> column(out, resourceName, column));
              resource.weakPrimaryKey().ifPresent(column -> column(out, resourceName, column));
            });
  }

  private boolean supports(FieldRef field) {
    return datasetScope.map(scope -> scope.supports(field)).orElse(true);
  }

  private boolean supports(CompiledRelationStep relation) {
    return datasetScope.map(scope -> scope.supports(relation)).orElse(true);
  }

  private boolean hasResource(String resource) {
    return datasetScope.map(scope -> scope.hasResource(resource)).orElse(true);
  }

  private void use(MappingInputRequirements.Builder out, String resource) {
    if (hasResource(resource)) {
      out.use(resource);
    }
  }

  private void column(MappingInputRequirements.Builder out, String resource, String column) {
    if (hasResource(resource)) {
      out.column(resource, column);
    }
  }

  private void allColumns(MappingInputRequirements.Builder out, String resource) {
    if (hasResource(resource)) {
      out.allColumns(resource);
    }
  }
}
