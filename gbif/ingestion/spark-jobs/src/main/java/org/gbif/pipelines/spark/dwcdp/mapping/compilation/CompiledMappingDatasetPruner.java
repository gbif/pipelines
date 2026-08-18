package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;

/**
 * Prunes an already compiled canonical mapping to the physical resources/columns declared by one
 * dataset's datapackage.json.
 *
 * <p>Compilation still happens against the complete official schema first. This class deliberately
 * does not re-run producer precedence after pruning: a producer that lost during canonical
 * compilation is never resurrected just because the canonical winner is unavailable in one dataset.
 * Dataset pruning is therefore an execution optimization, not a second mapping compiler.
 */
public final class CompiledMappingDatasetPruner {

  public CompiledMapping prune(CompiledMapping mapping, MappingDatasetScope scope) {
    Objects.requireNonNull(mapping, "mapping");
    Objects.requireNonNull(scope, "scope");

    List<CompiledTargetProducer> coreTargets = pruneProducers(mapping.coreTargets(), scope);
    List<CompiledTargetMerge> coreMerges = pruneMerges(mapping.coreTargetMerges(), scope);
    Set<String> coreMergeOwners = producerOwners(coreMerges);

    List<CompiledCoreFragment> coreFragments =
        mapping.coreFragments().stream()
            .filter(fragment -> supportsCoreFragmentStructure(fragment, scope))
            .map(
                fragment ->
                    pruneCoreFragment(
                        fragment, scope, mergeFieldsForOwner(coreMerges, fragment.name())))
            .filter(
                fragment ->
                    !fragment.targets().isEmpty() || coreMergeOwners.contains(fragment.name()))
            .toList();

    Set<String> visibleCoreTargets = new LinkedHashSet<>();
    coreTargets.forEach(producer -> visibleCoreTargets.add(producer.targetTerm()));
    coreMerges.forEach(merge -> visibleCoreTargets.add(merge.targetTerm()));

    List<CompiledExtension> extensions =
        mapping.extensions().stream()
            .map(extension -> pruneExtension(extension, scope))
            .flatMap(Optional::stream)
            .toList();

    return new CompiledMapping(
        mapping.name(),
        mapping.coreType(),
        mapping.coreSourceResource(),
        coreTargets,
        coreFragments,
        coreMerges,
        extensions,
        mapping.coreDecisions().stream()
            .filter(decision -> visibleCoreTargets.contains(decision.targetTerm()))
            .toList());
  }

  private Optional<CompiledExtension> pruneExtension(
      CompiledExtension extension, MappingDatasetScope scope) {
    List<CompiledTargetMerge> merges = pruneMerges(extension.targetMerges(), scope);
    Set<String> mergeOwners = producerOwners(merges);

    List<CompiledFragment> structurallyAvailable =
        extension.fragments().stream()
            .filter(fragment -> supportsExtensionFragmentStructure(fragment, scope))
            .map(
                fragment ->
                    pruneFragmentTargets(
                        fragment, scope, mergeFieldsForOwner(merges, fragment.name())))
            .toList();

    boolean anyTarget =
        !merges.isEmpty()
            || structurallyAvailable.stream().anyMatch(fragment -> !fragment.targets().isEmpty());
    if (!anyTarget) {
      return Optional.empty();
    }

    List<CompiledFragment> fragments = new ArrayList<>();
    if (extension.rowComposition() == ExtensionRowComposition.ENRICH) {
      CompiledFragment originalBase =
          extension.fragments().stream()
              .filter(fragment -> fragment.rowIdentity().isPresent())
              .findFirst()
              .orElse(extension.fragments().get(0));
      CompiledFragment base =
          structurallyAvailable.stream()
              .filter(fragment -> fragment.name().equals(originalBase.name()))
              .findFirst()
              .orElseGet(
                  () ->
                      structurallyAvailable.stream()
                          .filter(fragment -> fragment.rowIdentity().isPresent())
                          .findFirst()
                          .orElse(null));
      // ENRICH cannot manufacture rows without a row-defining fragment. If the canonical base is
      // unavailable for this dataset, an explicitly configured fallback rowIdentity fragment may
      // take over (e.g. Humboldt survey row when survey-target fan-out tables are absent).
      if (base == null) {
        return Optional.empty();
      }
      fragments.add(base);
      structurallyAvailable.stream()
          .filter(fragment -> !fragment.name().equals(base.name()))
          // ENRICH must have exactly one row-defining fragment. Alternative rowIdentity fragments
          // are dataset fallbacks for the base, not additional enrichment fragments.
          .filter(fragment -> fragment.rowIdentity().isEmpty())
          .filter(
              fragment -> !fragment.targets().isEmpty() || mergeOwners.contains(fragment.name()))
          .forEach(fragments::add);
    } else {
      structurallyAvailable.stream()
          .filter(
              fragment -> !fragment.targets().isEmpty() || mergeOwners.contains(fragment.name()))
          .forEach(fragments::add);
    }

    if (fragments.isEmpty()) {
      return Optional.empty();
    }

    Set<String> visibleTargets = new LinkedHashSet<>();
    fragments.forEach(
        fragment ->
            fragment.targets().forEach(producer -> visibleTargets.add(producer.targetTerm())));
    merges.forEach(merge -> visibleTargets.add(merge.targetTerm()));

    return Optional.of(
        new CompiledExtension(
            extension.rowType(),
            extension.rowComposition(),
            extension.maxRowsPerParent(),
            merges,
            fragments,
            extension.decisions().stream()
                .filter(decision -> visibleTargets.contains(decision.targetTerm()))
                .toList()));
  }

  private static CompiledCoreFragment pruneCoreFragment(
      CompiledCoreFragment fragment,
      MappingDatasetScope scope,
      Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> mergeFields) {
    List<CompiledTargetProducer> targets = pruneProducers(fragment.targets(), scope);
    Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> requiredFields =
        supportedFields(targetFields(targets), scope);
    requiredFields.addAll(supportedFields(mergeFields, scope));
    return new CompiledCoreFragment(
        fragment.name(),
        fragment.sourceResource(),
        fragment.path(),
        pruneUnreachableRelations(fragment.relations(), requiredFields),
        targets);
  }

  private static CompiledFragment pruneFragmentTargets(
      CompiledFragment fragment,
      MappingDatasetScope scope,
      Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> mergeFields) {
    List<CompiledTargetProducer> targets = pruneProducers(fragment.targets(), scope);
    Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> requiredFields =
        supportedFields(targetFields(targets), scope);
    requiredFields.addAll(supportedFields(mergeFields, scope));
    requiredFields.add(fragment.scopeKey());
    Optional<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> rowIdentity =
        fragment.rowIdentity().filter(scope::supports);
    Optional<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> rowMatch =
        fragment.rowMatch().filter(scope::supports);
    rowIdentity.ifPresent(requiredFields::add);
    rowMatch.ifPresent(requiredFields::add);
    return new CompiledFragment(
        fragment.name(),
        fragment.rowType(),
        fragment.sourceResource(),
        fragment.path(),
        pruneUnreachableRelations(fragment.relations(), requiredFields),
        fragment.scopeKey(),
        rowIdentity,
        rowMatch,
        targets);
  }

  private static Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef>
      mergeFieldsForOwner(List<CompiledTargetMerge> merges, String owner) {
    List<CompiledTargetProducer> owned =
        merges.stream()
            .flatMap(merge -> merge.producers().stream())
            .filter(producer -> producer.owner().equals(owner))
            .toList();
    return targetFields(owned);
  }

  private static Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> supportedFields(
      Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> fields,
      MappingDatasetScope scope) {
    Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> supported =
        new LinkedHashSet<>();
    fields.stream().filter(scope::supports).forEach(supported::add);
    return supported;
  }

  private static Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> targetFields(
      List<CompiledTargetProducer> targets) {
    Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> fields = new LinkedHashSet<>();
    for (CompiledTargetProducer target : targets) {
      target.sources().forEach(source -> fields.add(source.field()));
      target.contributionIdentity().ifPresent(source -> fields.add(source.field()));
      target.orderBy().ifPresent(source -> fields.add(source.field()));
    }
    return fields;
  }

  /**
   * Retains only relation steps that are on a path to a surviving physical dependency.
   *
   * <p>Relation requirement controls runtime behavior when a needed relation is unavailable; it is
   * not itself a reason to execute an otherwise dead join. A later required field always carries
   * the complete SchemaPath prefix, so keeping relations referenced by required field paths also
   * preserves every intermediate hop needed to reach that field.
   */
  private static List<CompiledRelationStep> pruneUnreachableRelations(
      List<CompiledRelationStep> relations,
      Set<org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef> requiredFields) {
    return relations.stream()
        .filter(
            step ->
                requiredFields.stream()
                    .anyMatch(field -> field.path().relations().contains(step.relation())))
        .toList();
  }

  private static List<CompiledTargetMerge> pruneMerges(
      List<CompiledTargetMerge> merges, MappingDatasetScope scope) {
    List<CompiledTargetMerge> result = new ArrayList<>();
    for (CompiledTargetMerge merge : merges) {
      List<CompiledTargetProducer> producers = pruneProducers(merge.producers(), scope);
      if (!producers.isEmpty()) {
        result.add(new CompiledTargetMerge(merge.targetTerm(), merge.aggregation(), producers));
      }
    }
    return List.copyOf(result);
  }

  private static List<CompiledTargetProducer> pruneProducers(
      List<CompiledTargetProducer> producers, MappingDatasetScope scope) {
    return producers.stream()
        .map(producer -> pruneProducer(producer, scope))
        .flatMap(Optional::stream)
        .toList();
  }

  private static Optional<CompiledTargetProducer> pruneProducer(
      CompiledTargetProducer producer, MappingDatasetScope scope) {
    List<CompiledSourceField> sources = pruneSources(producer, scope);
    if (sources.isEmpty() || sources.stream().noneMatch(scope::supports)) {
      return Optional.empty();
    }
    if (producer.contributionIdentity().isPresent()
        && !scope.supports(producer.contributionIdentity().orElseThrow())) {
      return Optional.empty();
    }
    if (producer.orderBy().isPresent() && !scope.supports(producer.orderBy().orElseThrow())) {
      return Optional.empty();
    }
    return Optional.of(
        new CompiledTargetProducer(
            producer.targetTerm(),
            producer.owner(),
            producer.sourceMode(),
            producer.aggregation(),
            sources,
            producer.origin(),
            producer.contributionIdentity(),
            producer.orderBy()));
  }

  /**
   * Prunes source alternatives without violating aggregation source-position contracts.
   *
   * <p>Labeled aggregations encode semantics by position. Missing resources therefore become null
   * source slots at execution time (via SparkPathResult.columnOrNull) rather than being removed:
   * [label, name, fallback] and [preferred, label, name, fallback]. Optional alternatives appended
   * after those fixed prefixes may still be pruned normally.
   */
  private static List<CompiledSourceField> pruneSources(
      CompiledTargetProducer producer, MappingDatasetScope scope) {
    int fixedPrefix =
        producer.aggregation() instanceof ValueAggregation.PreferredLabeledOrFallback
            ? 4
            : producer.aggregation() instanceof ValueAggregation.LabeledOrFallback ? 3 : 0;

    if (fixedPrefix == 0) {
      return producer.sources().stream().filter(scope::supports).toList();
    }

    List<CompiledSourceField> sources = new ArrayList<>();
    for (int i = 0; i < producer.sources().size(); i++) {
      CompiledSourceField source = producer.sources().get(i);
      if (i < fixedPrefix || scope.supports(source)) {
        sources.add(source);
      }
    }
    return List.copyOf(sources);
  }

  private static boolean supportsCoreFragmentStructure(
      CompiledCoreFragment fragment, MappingDatasetScope scope) {
    // Relation reachability is decided after surviving producers are known. A dead REQUIRED
    // relation must not make an otherwise executable fragment disappear.
    return scope.hasResource(fragment.sourceResource());
  }

  private static boolean supportsExtensionFragmentStructure(
      CompiledFragment fragment, MappingDatasetScope scope) {
    // scopeKey is structural because extension rows cannot be attached without it. Other relation
    // resources are evaluated only if a surviving target/identity actually depends on their path.
    return scope.hasResource(fragment.sourceResource()) && scope.supports(fragment.scopeKey());
  }

  private static Set<String> producerOwners(List<CompiledTargetMerge> merges) {
    Set<String> owners = new LinkedHashSet<>();
    merges.forEach(merge -> merge.producers().forEach(producer -> owners.add(producer.owner())));
    return Set.copyOf(owners);
  }
}
