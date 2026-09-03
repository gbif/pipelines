package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ExtensionRowComposition;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;

/**
 * Prunes an already compiled canonical mapping to the physical resources/columns declared by one
 * dataset's datapackage.json.
 *
 * <p>Compilation still happens against the complete official schema first. This class deliberately
 * does not re-run producer precedence after pruning: a producer that lost during canonical
 * compilation is never resurrected just because the canonical winner is unavailable in one dataset.
 * Dataset pruning is therefore a physical-plan specialization step, not a second producer
 * precedence compiler. Every surviving Spark dependency must be declared by the dataset.
 */
public final class CompiledMappingDatasetPruner {

  public CompiledMapping prune(CompiledMapping mapping, MappingDatasetScope scope) {
    Objects.requireNonNull(mapping, "mapping");
    Objects.requireNonNull(scope, "scope");

    Map<String, List<CompiledRelationStep>> coreRelationsByOwner =
        mapping.coreFragments().stream()
            .collect(Collectors.toMap(CompiledCoreFragment::name, CompiledCoreFragment::relations));
    List<CompiledTargetProducer> coreTargets = pruneProducers(mapping.coreTargets(), scope);
    List<CompiledTargetMerge> coreMerges =
        pruneMerges(mapping.coreTargetMerges(), scope, coreRelationsByOwner);
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
        mapping.coreIdentity().flatMap(identity -> pruneProducer(identity, scope)),
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
    Map<String, List<CompiledRelationStep>> relationsByOwner =
        extension.fragments().stream()
            .collect(Collectors.toMap(CompiledFragment::name, CompiledFragment::relations));
    List<CompiledTargetMerge> merges =
        pruneMerges(extension.targetMerges(), scope, relationsByOwner);
    Set<String> mergeOwners = producerOwners(merges);

    List<CompiledFragment> structurallyAvailable =
        extension.fragments().stream()
            .filter(
                fragment ->
                    supportsExtensionFragmentStructure(fragment, scope, extension.rowComposition()))
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
      CompiledCoreFragment fragment, MappingDatasetScope scope, Set<FieldRef> mergeFields) {
    Predicate<FieldRef> available = fieldAvailability(scope, fragment.relations());
    List<CompiledTargetProducer> targets = pruneProducers(fragment.targets(), scope, available);
    Set<FieldRef> requiredFields = supportedFields(targetFields(targets), available);
    requiredFields.addAll(supportedFields(mergeFields, available));
    return new CompiledCoreFragment(
        fragment.name(),
        fragment.sourceResource(),
        fragment.path(),
        pruneUnreachableRelations(fragment.relations(), requiredFields, scope),
        targets);
  }

  private static CompiledFragment pruneFragmentTargets(
      CompiledFragment fragment, MappingDatasetScope scope, Set<FieldRef> mergeFields) {
    Predicate<FieldRef> available = fieldAvailability(scope, fragment.relations());
    List<CompiledTargetProducer> targets = pruneProducers(fragment.targets(), scope, available);
    Set<FieldRef> requiredFields = supportedFields(targetFields(targets), available);
    requiredFields.addAll(supportedFields(mergeFields, available));
    requiredFields.add(fragment.scopeKey());
    Optional<FieldRef> rowIdentity = fragment.rowIdentity().filter(available);
    Optional<FieldRef> rowMatch = fragment.rowMatch().filter(available);
    rowIdentity.ifPresent(requiredFields::add);
    rowMatch.ifPresent(requiredFields::add);
    return new CompiledFragment(
        fragment.name(),
        fragment.rowType(),
        fragment.sourceResource(),
        fragment.path(),
        pruneUnreachableRelations(fragment.relations(), requiredFields, scope),
        fragment.scopeKey(),
        rowIdentity,
        rowMatch,
        targets);
  }

  private static Set<FieldRef> mergeFieldsForOwner(List<CompiledTargetMerge> merges, String owner) {
    List<CompiledTargetProducer> owned =
        merges.stream()
            .flatMap(merge -> merge.producers().stream())
            .filter(producer -> producer.owner().equals(owner))
            .toList();
    return targetFields(owned);
  }

  private static Set<FieldRef> supportedFields(
      Set<FieldRef> fields, Predicate<FieldRef> available) {
    Set<FieldRef> supported = new LinkedHashSet<>();
    fields.stream().filter(available).forEach(supported::add);
    return supported;
  }

  private static Set<FieldRef> targetFields(List<CompiledTargetProducer> targets) {
    Set<FieldRef> fields = new LinkedHashSet<>();
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
      Set<FieldRef> requiredFields,
      MappingDatasetScope scope) {
    return relations.stream()
        // A relation may be on a surviving logical field path but still be impossible to execute
        // for this dataset (missing join key, selector, or declared filter dependency). Dataset
        // specialization must remove it before Spark planning.
        .filter(scope::supports)
        .filter(
            step ->
                requiredFields.stream()
                    .anyMatch(field -> field.path().relations().contains(step.relation())))
        .toList();
  }

  private static List<CompiledTargetMerge> pruneMerges(
      List<CompiledTargetMerge> merges,
      MappingDatasetScope scope,
      Map<String, List<CompiledRelationStep>> relationsByOwner) {
    List<CompiledTargetMerge> result = new ArrayList<>();
    for (CompiledTargetMerge merge : merges) {
      List<CompiledTargetProducer> producers =
          merge.producers().stream()
              .map(
                  producer ->
                      pruneProducer(
                          producer,
                          scope,
                          fieldAvailability(
                              scope, relationsByOwner.getOrDefault(producer.owner(), List.of()))))
              .flatMap(Optional::stream)
              .toList();
      if (!producers.isEmpty()) {
        result.add(new CompiledTargetMerge(merge.targetTerm(), merge.aggregation(), producers));
      }
    }
    return List.copyOf(result);
  }

  private static List<CompiledTargetProducer> pruneProducers(
      List<CompiledTargetProducer> producers, MappingDatasetScope scope) {
    return pruneProducers(producers, scope, scope::supports);
  }

  private static List<CompiledTargetProducer> pruneProducers(
      List<CompiledTargetProducer> producers,
      MappingDatasetScope scope,
      Predicate<FieldRef> available) {
    return producers.stream()
        .map(producer -> pruneProducer(producer, scope, available))
        .flatMap(Optional::stream)
        .toList();
  }

  private static Optional<CompiledTargetProducer> pruneProducer(
      CompiledTargetProducer producer, MappingDatasetScope scope) {
    return pruneProducer(producer, scope, scope::supports);
  }

  private static Optional<CompiledTargetProducer> pruneProducer(
      CompiledTargetProducer producer, MappingDatasetScope scope, Predicate<FieldRef> available) {
    List<CompiledSourceField> sources = pruneSources(producer, scope, available);
    boolean positional = fixedSourcePrefix(producer.aggregation()) > 0;
    if (sources.isEmpty()
        || (!positional && sources.stream().noneMatch(source -> available.test(source.field())))) {
      return Optional.empty();
    }
    if (producer.contributionIdentity().isPresent()
        && !available.test(producer.contributionIdentity().orElseThrow().field())) {
      return Optional.empty();
    }
    if (producer.orderBy().isPresent()
        && !available.test(producer.orderBy().orElseThrow().field())) {
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
      CompiledTargetProducer producer, MappingDatasetScope scope, Predicate<FieldRef> available) {
    int fixedPrefix = fixedSourcePrefix(producer.aggregation());

    if (fixedPrefix == 0) {
      return producer.sources().stream().filter(source -> available.test(source.field())).toList();
    }

    List<CompiledSourceField> sources = new ArrayList<>();
    for (int i = 0; i < producer.sources().size(); i++) {
      CompiledSourceField source = producer.sources().get(i);
      if (i < fixedPrefix || available.test(source.field())) {
        sources.add(source);
      }
    }
    return List.copyOf(sources);
  }

  private static int fixedSourcePrefix(ValueAggregation aggregation) {
    return aggregation instanceof ValueAggregation.PreferredLabeledOrFallback
        ? 4
        : aggregation instanceof ValueAggregation.LabeledOrFallback
            ? 3
            : aggregation instanceof ValueAggregation.FirstOrUrnFallback ? 2 : 0;
  }

  private static boolean supportsCoreFragmentStructure(
      CompiledCoreFragment fragment, MappingDatasetScope scope) {
    // Relation reachability is decided after surviving producers are known. A dead REQUIRED
    // relation must not make an otherwise executable fragment disappear.
    return scope.hasResource(fragment.sourceResource());
  }

  private static boolean supportsExtensionFragmentStructure(
      CompiledFragment fragment,
      MappingDatasetScope scope,
      ExtensionRowComposition rowComposition) {
    // scopeKey is always structural because extension rows cannot be attached without it.
    //
    // For UNION, a declared rowIdentity defines the independent physical child row. If that
    // identity path is unavailable, retaining the fragment would silently degrade it into a
    // different row set (for example occurrence-owned Multimedia becoming one routing-only row
    // per occurrence when occurrence-media is absent).
    //
    // ENRICH is different: rowIdentity fragments may deliberately act as alternative/fallback
    // row sources. Humboldt relies on this when target tables are absent and the physical survey
    // row becomes the base, so identity availability must continue to be resolved by the ENRICH
    // fallback selection below rather than rejected here.
    Predicate<FieldRef> available = fieldAvailability(scope, fragment.relations());
    return scope.hasResource(fragment.sourceResource())
        && available.test(fragment.scopeKey())
        && (rowComposition != ExtensionRowComposition.UNION
            || (fragment.rowIdentity().map(available::test).orElse(true)
                && fragment.rowMatch().map(available::test).orElse(true)));
  }

  private static Predicate<FieldRef> fieldAvailability(
      MappingDatasetScope scope, List<CompiledRelationStep> relations) {
    Map<SchemaRelation, CompiledRelationStep> stepsByRelation =
        relations.stream()
            .collect(Collectors.toMap(CompiledRelationStep::relation, step -> step, (a, b) -> a));
    return field -> {
      if (!scope.supports(field)) {
        return false;
      }
      for (SchemaRelation relation : field.path().relations()) {
        CompiledRelationStep step = stepsByRelation.get(relation);
        if (step != null && !scope.supports(step)) {
          return false;
        }
      }
      return true;
    };
  }

  private static Set<String> producerOwners(List<CompiledTargetMerge> merges) {
    Set<String> owners = new LinkedHashSet<>();
    merges.forEach(merge -> merge.producers().forEach(producer -> owners.add(producer.owner())));
    return Set.copyOf(owners);
  }
}
