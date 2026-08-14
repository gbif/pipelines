package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
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

    Resolution core = resolveTargets(
        "core:" + plan.coreSourceResource(),
        plan.coreFields().stream().map(field -> compileTarget("core", field)).toList());

    List<CompiledExtension> extensions =
        plan.extensions().stream().map(extension -> compileExtensionInternal(extension)).toList();
    List<MappingDecision> problems = new ArrayList<>();
    problems.addAll(core.decisions().stream().filter(MappingDecision::problem).toList());
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
        core.selected(),
        extensions,
        core.decisions());
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
                resolution.selected()));
      }
      return new CompiledExtension(
          extension.rowType(),
          extension.rowComposition(),
          extension.maxRowsPerParent(),
          resolvedFragments,
          decisions);
    }

    List<CompiledTargetProducer> candidates =
        rawFragments.stream().flatMap(fragment -> fragment.targets().stream()).toList();
    Resolution resolution = resolveTargets("extension:" + extension.rowType(), candidates);

    List<CompiledTargetProducer> selected = resolution.selected();
    List<CompiledFragment> resolvedFragments = rawFragments.stream()
        .map(fragment -> new CompiledFragment(
            fragment.name(),
            fragment.rowType(),
            fragment.sourceResource(),
            fragment.path(),
            fragment.relations(),
            fragment.scopeKey(),
            fragment.rowIdentity(),
            fragment.targets().stream().filter(selected::contains).toList()))
        .toList();

    return new CompiledExtension(
        extension.rowType(),
        extension.rowComposition(),
        extension.maxRowsPerParent(),
        resolvedFragments,
        resolution.decisions());
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

  private CompiledFragment compileFragment(ExtensionFragment fragment) {
    Objects.requireNonNull(fragment, "fragment");
    SchemaPath path = SchemaPath.root(fragment.sourceResource());
    List<CompiledRelationStep> relations = new ArrayList<>();
    for (RelationStep step : fragment.relations()) {
      SchemaRelation relation = resolveRelation(path.currentResource(), step);
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
        targets);
  }

  private static CompiledTargetProducer compileTarget(String owner, TargetFieldMapping field) {
    return new CompiledTargetProducer(
        field.targetTerm(),
        owner,
        field.sourceMode(),
        field.aggregation(),
        field.sources().stream().map(CompiledSourceField::new).toList(),
        field.origin());
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

  private SchemaRelation resolveRelation(String sourceResource, RelationStep step) {
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
  }

  private record Resolution(
      List<CompiledTargetProducer> selected, List<MappingDecision> decisions) {
    private Resolution {
      selected = List.copyOf(selected);
      decisions = List.copyOf(decisions);
    }
  }
}
