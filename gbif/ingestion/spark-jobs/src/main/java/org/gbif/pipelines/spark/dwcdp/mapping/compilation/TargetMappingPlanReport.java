package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CardinalityStrategy;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;

/** Structured target-first report model shared by text and JSON renderers. */
public record TargetMappingPlanReport(
    String mapping,
    String view,
    String detail,
    String coreType,
    String coreSourceResource,
    List<Scope> scopes) {

  public TargetMappingPlanReport {
    scopes = List.copyOf(scopes);
  }

  public record Scope(
      String kind,
      String name,
      String rowComposition,
      Integer maxRowsPerParent,
      Integer targetsAvailable,
      int targetsTotal,
      List<Target> targets) {
    public Scope {
      targets = List.copyOf(targets);
    }
  }

  public record Target(String term, String merge, Decision decision, List<Producer> producers) {
    public Target {
      producers = List.copyOf(producers);
    }
  }

  public record Decision(String type, String explanation) {}

  public record Producer(
      String owner,
      String origin,
      Integer inferredDepth,
      String valueType,
      String sourceMode,
      String aggregation,
      String expression,
      String contributionIdentity,
      String orderBy,
      List<PathStep> path,
      List<String> sources) {
    public Producer {
      path = List.copyOf(path);
      sources = List.copyOf(sources);
    }
  }

  public record PathStep(
      String sourceResource,
      String sourceColumn,
      String targetResource,
      String targetColumn,
      String relationType,
      boolean weak,
      String requirement,
      String cardinality,
      String predicate,
      boolean filtered) {}

  public static TargetMappingPlanReport from(
      CompiledMapping mapping,
      Optional<MappingDatasetScope> datasetScope,
      TargetMappingPlanRenderer.Detail detail) {
    Objects.requireNonNull(mapping, "mapping");
    Objects.requireNonNull(datasetScope, "datasetScope");
    Objects.requireNonNull(detail, "detail");

    List<Scope> scopes = new ArrayList<>();
    scopes.add(
        scope(
            "CORE",
            mapping.coreType().toString(),
            collectCoreTargets(mapping),
            mergeIndex(mapping.coreTargetMerges()),
            decisionIndex(mapping.coreDecisions()),
            ownerRelations(mapping.coreFragments()),
            datasetScope,
            detail,
            null));

    for (CompiledExtension extension : mapping.extensions()) {
      Map<String, List<CompiledTargetProducer>> targets = collectExtensionTargets(extension);
      if (!hasVisibleTargets(targets, datasetScope)) {
        continue;
      }
      scopes.add(
          scope(
              "EXTENSION",
              extension.rowType(),
              targets,
              mergeIndex(extension.targetMerges()),
              decisionIndex(extension.decisions()),
              ownerRelations(extension.fragments()),
              datasetScope,
              detail,
              extension));
    }

    return new TargetMappingPlanReport(
        mapping.name(),
        datasetScope.isPresent() ? "dataset" : "master schema",
        detail.name().toLowerCase(),
        mapping.coreType().toString(),
        mapping.coreSourceResource(),
        scopes);
  }

  private static Scope scope(
      String kind,
      String name,
      Map<String, List<CompiledTargetProducer>> targets,
      Map<String, CompiledTargetMerge> merges,
      Map<String, MappingDecision> decisions,
      Map<String, List<CompiledRelationStep>> relationsByOwner,
      Optional<MappingDatasetScope> datasetScope,
      TargetMappingPlanRenderer.Detail detail,
      CompiledExtension extension) {
    List<String> visibleTargets =
        targets.entrySet().stream()
            .filter(entry -> hasVisibleProducer(entry.getValue(), datasetScope))
            .map(Map.Entry::getKey)
            .sorted()
            .toList();

    List<Target> reports =
        visibleTargets.stream()
            .map(
                target -> {
                  List<CompiledTargetProducer> producers =
                      targets.get(target).stream()
                          .filter(
                              producer ->
                                  datasetScope.map(scope -> scope.supports(producer)).orElse(true))
                          .toList();
                  CompiledTargetMerge merge = merges.get(target);
                  MappingDecision decision = decisions.get(target);
                  return new Target(
                      target,
                      merge == null ? null : formatAggregation(merge.aggregation()),
                      detail == TargetMappingPlanRenderer.Detail.DETAILED && decision != null
                          ? new Decision(decision.type().name(), decision.explanation())
                          : null,
                      producers.stream()
                          .map(
                              producer ->
                                  producer(
                                      producer,
                                      relationsByOwner.getOrDefault(producer.owner(), List.of()),
                                      datasetScope,
                                      detail))
                          .toList());
                })
            .toList();

    return new Scope(
        kind,
        name,
        extension == null ? null : extension.rowComposition().name(),
        extension == null ? null : extension.maxRowsPerParent().orElse(null),
        datasetScope.isPresent() ? visibleTargets.size() : null,
        targets.size(),
        reports);
  }

  private static Producer producer(
      CompiledTargetProducer producer,
      List<CompiledRelationStep> relations,
      Optional<MappingDatasetScope> datasetScope,
      TargetMappingPlanRenderer.Detail detail) {
    List<String> sources =
        producer.sources().stream()
            .filter(source -> datasetScope.map(scope -> scope.supports(source)).orElse(true))
            .map(CompiledSourceField::describe)
            .toList();

    boolean detailed = detail == TargetMappingPlanRenderer.Detail.DETAILED;
    String valueType = producer.expressionValue() ? "EXPRESSION" : "AGGREGATED";
    return new Producer(
        detailed ? producer.owner() : null,
        detailed ? producer.origin().name() : null,
        detailed && producer.origin() == TargetFieldMapping.Origin.INFERRED
            ? producer.pathDepth()
            : null,
        valueType,
        producer.expressionValue() ? null : producer.sourceMode().name(),
        producer.expressionValue() ? null : formatAggregation(producer.aggregation()),
        producer.expressionValue() ? producer.expression().toString() : null,
        detailed
            ? producer.contributionIdentity().map(CompiledSourceField::describe).orElse(null)
            : null,
        detailed ? producer.orderBy().map(CompiledSourceField::describe).orElse(null) : null,
        relations.stream().map(TargetMappingPlanReport::pathStep).toList(),
        sources);
  }

  private static PathStep pathStep(CompiledRelationStep relation) {
    SchemaRelation schema = relation.relation();
    return new PathStep(
        schema.sourceResource(),
        schema.sourceColumn(),
        schema.targetResource(),
        schema.targetColumn(),
        relation.explicitColumns() ? "EXPLICIT" : "SCHEMA",
        schema.weak(),
        relation.requirement().name(),
        relation.cardinalityStrategy().map(TargetMappingPlanReport::formatCardinality).orElse(null),
        schema.predicate().map(Object::toString).orElse(null),
        relation.filter().isPresent());
  }

  private static Map<String, List<CompiledTargetProducer>> collectCoreTargets(
      CompiledMapping mapping) {
    List<CompiledTargetProducer> all = new ArrayList<>(mapping.coreTargets());
    mapping.coreFragments().forEach(fragment -> all.addAll(fragment.targets()));
    return groupTargets(all);
  }

  private static Map<String, List<CompiledTargetProducer>> collectExtensionTargets(
      CompiledExtension extension) {
    return groupTargets(
        extension.fragments().stream().flatMap(fragment -> fragment.targets().stream()).toList());
  }

  private static Map<String, List<CompiledTargetProducer>> groupTargets(
      List<CompiledTargetProducer> producers) {
    return producers.stream()
        .collect(
            Collectors.groupingBy(
                CompiledTargetProducer::targetTerm, LinkedHashMap::new, Collectors.toList()));
  }

  private static Map<String, CompiledTargetMerge> mergeIndex(List<CompiledTargetMerge> merges) {
    return merges.stream()
        .collect(
            Collectors.toMap(
                CompiledTargetMerge::targetTerm,
                Function.identity(),
                (left, right) -> left,
                LinkedHashMap::new));
  }

  private static Map<String, MappingDecision> decisionIndex(List<MappingDecision> decisions) {
    return decisions.stream()
        .collect(
            Collectors.toMap(
                MappingDecision::targetTerm,
                Function.identity(),
                (left, right) -> left,
                LinkedHashMap::new));
  }

  private static Map<String, List<CompiledRelationStep>> ownerRelations(
      List<CompiledCoreFragment> fragments) {
    return fragments.stream()
        .collect(
            Collectors.toMap(
                CompiledCoreFragment::name,
                CompiledCoreFragment::relations,
                (left, right) -> left,
                LinkedHashMap::new));
  }

  private static Map<String, List<CompiledRelationStep>> ownerRelations(
      Iterable<CompiledFragment> fragments) {
    Map<String, List<CompiledRelationStep>> out = new LinkedHashMap<>();
    for (CompiledFragment fragment : fragments) {
      out.put(fragment.name(), fragment.relations());
    }
    return out;
  }

  private static boolean hasVisibleTargets(
      Map<String, List<CompiledTargetProducer>> targets,
      Optional<MappingDatasetScope> datasetScope) {
    return targets.values().stream()
        .anyMatch(producers -> hasVisibleProducer(producers, datasetScope));
  }

  private static boolean hasVisibleProducer(
      List<CompiledTargetProducer> producers, Optional<MappingDatasetScope> datasetScope) {
    return datasetScope.isEmpty()
        || producers.stream().anyMatch(datasetScope.orElseThrow()::supports);
  }

  static String formatCardinality(CardinalityStrategy strategy) {
    if (strategy instanceof CardinalityStrategy.FanOut) {
      return "FAN_OUT";
    }
    if (strategy instanceof CardinalityStrategy.ExactlyOne) {
      return "EXACTLY_ONE";
    }
    if (strategy instanceof CardinalityStrategy.Select select) {
      return "SELECT(" + select.selector() + ")";
    }
    if (strategy instanceof CardinalityStrategy.Combine combine) {
      return "COMBINE(" + formatAggregation(combine.aggregation()) + ")";
    }
    return strategy.toString();
  }

  static String formatAggregation(ValueAggregation aggregation) {
    if (aggregation instanceof ValueAggregation.FirstNonNull) {
      return "FIRST_NON_NULL";
    }
    if (aggregation instanceof ValueAggregation.ExactlyOne) {
      return "EXACTLY_ONE";
    }
    if (aggregation instanceof ValueAggregation.Delimited delimited) {
      return "DELIMITED('" + delimited.delimiter() + "', distinct=" + delimited.distinct() + ")";
    }
    if (aggregation instanceof ValueAggregation.LabeledOrFallback labeled) {
      return "LABELED_OR_FALLBACK('" + labeled.separator() + "')";
    }
    if (aggregation instanceof ValueAggregation.PreferredLabeledOrFallback preferred) {
      return "PREFERRED_LABELED_OR_FALLBACK('" + preferred.separator() + "')";
    }
    if (aggregation instanceof ValueAggregation.Named named) {
      return named.name();
    }
    return aggregation.toString();
  }
}
