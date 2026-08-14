package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.mapping.CoreFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetMerge;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.ExtensionMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.FieldRef;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationCardinality;
import org.gbif.pipelines.spark.dwcdp.mapping.RelationStep;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaPath;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping;

/**
 * Resolves declarative mapping navigation and target ownership against a DwC-DP schema graph.
 * Ambiguity is rejected here, before any Spark Dataset is touched.
 */
public final class MappingCompiler {
  private final SchemaGraph graph;

  public MappingCompiler(SchemaGraph graph) {
    this.graph = Objects.requireNonNull(graph, "graph");
  }

  public CompiledMapping compile(MappingPlan plan) {
    Objects.requireNonNull(plan, "plan");

    List<MappingDecision> structuralProblems = validateFragmentScopes(plan.extensions());
    if (!structuralProblems.isEmpty()) {
      throw new MappingCompilationException(structuralProblems);
    }

    List<CompiledTargetProducer> rootCoreTargets =
        plan.coreFields().stream().map(field -> compileTarget("core", field)).toList();
    List<CompiledCoreFragment> rawCoreFragments =
        plan.coreFragments().stream().map(this::compileCoreFragment).toList();
    List<CompiledTargetProducer> coreCandidates = new ArrayList<>(rootCoreTargets);
    rawCoreFragments.forEach(fragment -> coreCandidates.addAll(fragment.targets()));

    Map<String, TargetMerge> mergeDeclarations =
        plan.coreTargetMerges().stream()
            .collect(Collectors.toMap(
                TargetMerge::targetTerm,
                merge -> merge,
                (left, right) -> left,
                LinkedHashMap::new));
    List<CompiledTargetMerge> coreTargetMerges = new ArrayList<>();
    List<MappingDecision> mergeDecisions = new ArrayList<>();
    for (TargetMerge merge : mergeDeclarations.values()) {
      List<CompiledTargetProducer> producers =
          coreCandidates.stream()
              .filter(candidate -> candidate.targetTerm().equals(merge.targetTerm()))
              .toList();
      if (!producers.isEmpty()) {
        coreTargetMerges.add(
            new CompiledTargetMerge(merge.targetTerm(), merge.aggregation(), producers));
        mergeDecisions.add(
            new MappingDecision(
                "core:" + plan.coreSourceResource(),
                merge.targetTerm(),
                MappingDecisionType.EXPLICIT_MERGE,
                Optional.empty(),
                producers,
                "Independent core producers are merged using explicitly declared semantics: "
                    + merge.aggregation()));
      }
    }

    List<CompiledTargetProducer> nonMergeCandidates =
        coreCandidates.stream()
            .filter(candidate -> !mergeDeclarations.containsKey(candidate.targetTerm()))
            .toList();
    Resolution core = resolveTargets("core:" + plan.coreSourceResource(), nonMergeCandidates);
    List<CompiledTargetProducer> selectedCore = new ArrayList<>(core.selected());
    coreTargetMerges.forEach(merge -> selectedCore.addAll(merge.producers()));
    List<CompiledTargetProducer> selectedRootCoreTargets =
        rootCoreTargets.stream().filter(selectedCore::contains).toList();
    List<CompiledCoreFragment> coreFragments =
        rawCoreFragments.stream()
            .map(
                fragment ->
                    new CompiledCoreFragment(
                        fragment.name(),
                        fragment.sourceResource(),
                        fragment.path(),
                        fragment.relations(),
                        fragment.targets().stream().filter(selectedCore::contains).toList()))
            .toList();

    List<CompiledExtension> extensions =
        plan.extensions().stream().map(extension -> compileExtensionInternal(extension)).toList();
    List<MappingDecision> problems = new ArrayList<>();
    problems.addAll(core.decisions().stream().filter(MappingDecision::problem).toList());
    problems.addAll(mergeDecisions.stream().filter(MappingDecision::problem).toList());
    extensions.forEach(
        extension -> problems.addAll(
            extension.decisions().stream().filter(MappingDecision::problem).toList()));
    if (!problems.isEmpty()) {
      throw new MappingCompilationException(problems);
    }

    return new CompiledMapping(
        plan.name(),
        plan.coreType(),
        plan.coreSourceResource(),
        selectedRootCoreTargets,
        coreFragments,
        coreTargetMerges,
        extensions,
        java.util.stream.Stream.concat(core.decisions().stream(), mergeDecisions.stream()).toList());
  }

  public CompiledExtension compile(ExtensionMapping extension) {
    Objects.requireNonNull(extension, "extension");
    List<MappingDecision> structuralProblems = validateFragmentScopes(List.of(extension));
    if (!structuralProblems.isEmpty()) {
      throw new MappingCompilationException(structuralProblems);
    }
    CompiledExtension compiled = compileExtensionInternal(extension);
    List<MappingDecision> problems = compiled.decisions().stream()
        .filter(MappingDecision::problem)
        .toList();
    if (!problems.isEmpty()) {
      throw new MappingCompilationException(problems);
    }
    return compiled;
  }

  private CompiledExtension compileExtensionInternal(ExtensionMapping extension) {
    Objects.requireNonNull(extension, "extension");
    List<CompiledFragment> rawFragments =
        extension.fragments().stream().map(this::compileFragment).toList();

    Map<String, TargetMerge> mergeDeclarations =
        extension.targetMerges().stream()
            .collect(Collectors.toMap(
                TargetMerge::targetTerm,
                merge -> merge,
                (left, right) -> left,
                LinkedHashMap::new));

    if (extension.rowComposition()
        == org.gbif.pipelines.spark.dwcdp.mapping.ExtensionRowComposition.UNION) {
      List<CompiledFragment> resolvedFragments = new ArrayList<>();
      List<MappingDecision> decisions = new ArrayList<>();
      for (CompiledFragment fragment : rawFragments) {
        Resolution resolution =
            resolveTargets(
                "extension:" + extension.rowType() + "/fragment:" + fragment.name(),
                fragment.targets());
        decisions.addAll(resolution.decisions());
        resolvedFragments.add(
            new CompiledFragment(
                fragment.name(),
                fragment.rowType(),
                fragment.sourceResource(),
                fragment.path(),
                fragment.relations(),
                fragment.scopeKey(),
                fragment.rowIdentity(),
                fragment.rowMatch(),
                resolution.selected()));
      }
      return new CompiledExtension(
          extension.rowType(),
          extension.rowComposition(),
          extension.maxRowsPerParent(),
          List.of(),
          resolvedFragments,
          decisions);
    }

    List<CompiledTargetProducer> candidates =
        rawFragments.stream().flatMap(fragment -> fragment.targets().stream()).toList();
    List<CompiledTargetMerge> targetMerges = new ArrayList<>();
    List<MappingDecision> mergeDecisions = new ArrayList<>();
    for (TargetMerge merge : mergeDeclarations.values()) {
      List<CompiledTargetProducer> producers =
          candidates.stream()
              .filter(candidate -> candidate.targetTerm().equals(merge.targetTerm()))
              .toList();
      if (!producers.isEmpty()) {
        targetMerges.add(new CompiledTargetMerge(merge.targetTerm(), merge.aggregation(), producers));
        mergeDecisions.add(
            new MappingDecision(
                "extension:" + extension.rowType(),
                merge.targetTerm(),
                MappingDecisionType.EXPLICIT_MERGE,
                Optional.empty(),
                producers,
                "Independent extension producers are merged using explicitly declared semantics: "
                    + merge.aggregation()));
      }
    }

    List<CompiledTargetProducer> nonMergeCandidates =
        candidates.stream()
            .filter(candidate -> !mergeDeclarations.containsKey(candidate.targetTerm()))
            .toList();
    Resolution resolution = resolveTargets("extension:" + extension.rowType(), nonMergeCandidates);
    List<CompiledTargetProducer> selected = new ArrayList<>(resolution.selected());
    targetMerges.forEach(merge -> selected.addAll(merge.producers()));
    List<CompiledFragment> resolvedFragments = rawFragments.stream()
        .map(fragment -> new CompiledFragment(
            fragment.name(),
            fragment.rowType(),
            fragment.sourceResource(),
            fragment.path(),
            fragment.relations(),
            fragment.scopeKey(),
            fragment.rowIdentity(),
            fragment.rowMatch(),
            fragment.targets().stream().filter(selected::contains).toList()))
        .toList();

    return new CompiledExtension(
        extension.rowType(),
        extension.rowComposition(),
        extension.maxRowsPerParent(),
        targetMerges,
        resolvedFragments,
        java.util.stream.Stream.concat(resolution.decisions().stream(), mergeDecisions.stream()).toList());
  }

  private List<MappingDecision> validateFragmentScopes(List<ExtensionMapping> extensions) {
    List<MappingDecision> problems = new ArrayList<>();
    for (ExtensionMapping extension : extensions) {
      for (ExtensionFragment fragment : extension.fragments()) {
        if (fragment.scopeKeyColumn().isPresent()) {
          continue;
        }
        boolean hasPrimaryKey =
            graph.resource(fragment.sourceResource()).flatMap(resource -> resource.primaryKey()).isPresent();
        if (!hasPrimaryKey) {
          problems.add(
              new MappingDecision(
                  "fragment:" + fragment.name(),
                  "<scope>",
                  MappingDecisionType.MISSING_FRAGMENT_SCOPE,
                  Optional.empty(),
                  List.of(),
                  "Fragment source '"
                      + fragment.sourceResource()
                      + "' has no primary key and no explicit scope key."));
        }
      }
    }
    return problems;
  }

  private CompiledCoreFragment compileCoreFragment(CoreFragment fragment) {
    Objects.requireNonNull(fragment, "fragment");
    SchemaPath path = SchemaPath.root(fragment.sourceResource());
    List<CompiledRelationStep> relations = new ArrayList<>();
    for (RelationStep step : fragment.relations()) {
      SchemaRelation relation = resolveRelation("core-fragment:" + fragment.name(), path.currentResource(), step);
      relations.add(
          new CompiledRelationStep(
              relation,
              step.explicitColumns(),
              step.requirement(),
              step.cardinalityStrategy(),
              step.filter()));
      path = path.append(relation);
    }
    List<CompiledTargetProducer> targets =
        fragment.fields().stream().map(field -> compileTarget(fragment.name(), field)).toList();
    return new CompiledCoreFragment(
        fragment.name(), fragment.sourceResource(), path, relations, targets);
  }

  private CompiledFragment compileFragment(ExtensionFragment fragment) {
    Objects.requireNonNull(fragment, "fragment");
    SchemaPath path = SchemaPath.root(fragment.sourceResource());
    List<CompiledRelationStep> relations = new ArrayList<>();
    for (RelationStep step : fragment.relations()) {
      SchemaRelation relation = resolveRelation("extension-fragment:" + fragment.name(), path.currentResource(), step);
      relations.add(
          new CompiledRelationStep(
              relation,
              step.explicitColumns(),
              step.requirement(),
              step.cardinalityStrategy(),
              step.filter()));
      path = path.append(relation);
    }

    SchemaPath sourcePath = SchemaPath.root(fragment.sourceResource());
    FieldRef scopeKey =
        fragment
            .scopeKeyColumn()
            .map(sourcePath::field)
            .orElseGet(
                () ->
                    graph
                        .resource(fragment.sourceResource())
                        .flatMap(resource -> resource.primaryKey())
                        .map(sourcePath::field)
                        .orElseThrow(
                            () ->
                                new IllegalStateException(
                                    "Fragment scope validation did not run for source: "
                                        + fragment.sourceResource())));
    Optional<FieldRef> rowIdentity = fragment.rowIdentity();
    Optional<FieldRef> rowMatch = fragment.rowMatch();
    List<CompiledTargetProducer> targets =
        fragment.fields().stream().map(field -> compileTarget(fragment.name(), field)).toList();

    return new CompiledFragment(
        fragment.name(),
        fragment.rowType(),
        fragment.sourceResource(),
        path,
        relations,
        scopeKey,
        rowIdentity,
        rowMatch,
        targets);
  }

  private static CompiledTargetProducer compileTarget(String owner, TargetFieldMapping field) {
    return new CompiledTargetProducer(
        field.targetTerm(),
        owner,
        field.sourceMode(),
        field.aggregation(),
        field.sources().stream().map(CompiledSourceField::new).toList(),
        field.origin(),
        field.contributionIdentity().map(CompiledSourceField::new),
        field.orderBy().map(CompiledSourceField::new));
  }

  private static Resolution resolveTargets(String scope, List<CompiledTargetProducer> candidates) {
    Map<String, List<CompiledTargetProducer>> byTarget = candidates.stream()
        .collect(Collectors.groupingBy(
            CompiledTargetProducer::targetTerm,
            LinkedHashMap::new,
            Collectors.toList()));

    List<CompiledTargetProducer> selected = new ArrayList<>();
    List<MappingDecision> decisions = new ArrayList<>();

    for (Map.Entry<String, List<CompiledTargetProducer>> entry : byTarget.entrySet()) {
      String target = entry.getKey();
      List<CompiledTargetProducer> targetCandidates = entry.getValue();
      MappingDecision decision = decide(scope, target, targetCandidates);
      decisions.add(decision);
      decision.selected().ifPresent(selected::add);
    }
    return new Resolution(selected, decisions);
  }

  private static MappingDecision decide(
      String scope, String target, List<CompiledTargetProducer> candidates) {
    if (candidates.size() == 1) {
      return new MappingDecision(
          scope,
          target,
          MappingDecisionType.SINGLE_PRODUCER,
          Optional.of(candidates.get(0)),
          candidates,
          "Only one producer declares this target.");
    }

    List<CompiledTargetProducer> explicit = candidates.stream()
        .filter(candidate -> candidate.origin() == TargetFieldMapping.Origin.EXPLICIT)
        .toList();
    if (explicit.size() == 1) {
      CompiledTargetProducer winner = explicit.get(0);
      return new MappingDecision(
          scope,
          target,
          MappingDecisionType.EXPLICIT_OVERRIDE,
          Optional.of(winner),
          candidates,
          "The explicit producer outranks inferred direct-field producers.");
    }
    if (explicit.size() > 1) {
      return new MappingDecision(
          scope,
          target,
          MappingDecisionType.AMBIGUOUS_MULTIPLE_EXPLICIT,
          Optional.empty(),
          candidates,
          "Multiple explicit producers claim the same target and no merge semantics were declared.");
    }

    int minimumDepth = candidates.stream()
        .mapToInt(CompiledTargetProducer::pathDepth)
        .min()
        .orElseThrow();
    List<CompiledTargetProducer> closest = candidates.stream()
        .filter(candidate -> candidate.pathDepth() == minimumDepth)
        .toList();
    if (closest.size() == 1) {
      CompiledTargetProducer winner = closest.get(0);
      return new MappingDecision(
          scope,
          target,
          MappingDecisionType.INFERRED_CLOSEST,
          Optional.of(winner),
          candidates,
          "All producers are inferred; the closest source path wins (depth " + minimumDepth + ").");
    }

    return new MappingDecision(
        scope,
        target,
        MappingDecisionType.AMBIGUOUS_EQUAL_DEPTH,
        Optional.empty(),
        candidates,
        "Multiple inferred producers have the same closest path depth (" + minimumDepth
            + "); an explicit mapping is required.");
  }

  private SchemaRelation resolveRelation(String scope, String sourceResource, RelationStep step) {
    try {
      if (step.explicitColumns()) {
        String sourceColumn = step.sourceColumn().orElseThrow();
        String targetColumn = step.targetColumn().orElseThrow();
        if (!graph.hasResource(step.targetResource())) {
          throw new IllegalArgumentException(
              "Unknown relation target resource: " + step.targetResource());
        }
        if (!graph.hasColumn(sourceResource, sourceColumn)) {
          throw new IllegalArgumentException(
              "Explicit relation references unknown source field: "
                  + sourceResource
                  + "."
                  + sourceColumn);
        }
        if (!graph.hasColumn(step.targetResource(), targetColumn)) {
          throw new IllegalArgumentException(
              "Explicit relation references unknown target field: "
                  + step.targetResource()
                  + "."
                  + targetColumn);
        }
        return SchemaRelation.relation(
            sourceResource,
            sourceColumn,
            step.targetResource(),
            targetColumn,
            step.schemaPredicate().orElse(null),
            RelationCardinality.UNKNOWN);
      }
      return graph.resolve(
          sourceResource,
          step.targetResource(),
          step.viaColumn().orElse(null),
          step.schemaPredicate().orElse(null));
    } catch (IllegalArgumentException error) {
      throw relationCompilationException(scope, sourceResource, step, error);
    }
  }

  private MappingCompilationException relationCompilationException(
      String scope, String sourceResource, RelationStep step, IllegalArgumentException cause) {
    StringBuilder explanation = new StringBuilder(cause.getMessage());
    List<SchemaRelation> direct = graph.relationsFrom(sourceResource);
    if (!direct.isEmpty()) {
      explanation.append("\nDirect relations from ").append(sourceResource).append(':');
      direct.stream().limit(10).forEach(r -> explanation.append("\n  - ").append(describeRelation(r)));
    }
    List<List<SchemaRelation>> hints =
        graph.nearbyPaths(sourceResource, step.targetResource(), 3, 10);
    if (!hints.isEmpty()) {
      explanation.append("\nNearby schema paths (diagnostic hints only):");
      for (List<SchemaRelation> path : hints) {
        explanation.append("\n  - ")
            .append(path.stream().map(MappingCompiler::describeRelation).collect(Collectors.joining(" ; ")));
      }
    }
    MappingDecision problem =
        new MappingDecision(
            scope,
            "<relation:" + sourceResource + "->" + step.targetResource() + ">",
            MappingDecisionType.INVALID_RELATION,
            Optional.empty(),
            List.of(),
            explanation.toString());
    MappingCompilationException exception = new MappingCompilationException(List.of(problem));
    exception.initCause(cause);
    return exception;
  }

  private static String describeRelation(SchemaRelation relation) {
    return relation.sourceResource()
        + "."
        + relation.sourceColumn()
        + " -> "
        + relation.targetResource()
        + "."
        + relation.targetColumn()
        + relation.predicate().map(p -> " [predicate=" + p + "]").orElse("");
  }

  private record Resolution(
      List<CompiledTargetProducer> selected, List<MappingDecision> decisions) {
    private Resolution {
      selected = List.copyOf(selected);
      decisions = List.copyOf(decisions);
    }
  }
}
