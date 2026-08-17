package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import org.gbif.pipelines.spark.dwcdp.mapping.definition.Mapping;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.CardinalityStrategy;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaRelation;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.ValueAggregation;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.TargetFieldMapping;

/**
 * Target-first view of a compiled DwC-DP -> DwC-A plan.
 *
 * <p>Unlike {@link MappingTraceRenderer}, this renderer deliberately hides fragment assembly as the
 * primary structure. DwC-A targets are the aggregate roots; producer fragments, paths and compiler
 * decisions are supporting provenance underneath each target.
 */
public final class TargetMappingPlanRenderer {

  public enum Detail {
    COMPACT,
    DETAILED
  }

  private TargetMappingPlanRenderer() {}

  public static String render(CompiledMapping mapping, Detail detail) {
    return render(mapping, Optional.empty(), detail);
  }

  public static String render(
      CompiledMapping mapping, MappingDatasetScope datasetScope, Detail detail) {
    return render(mapping, Optional.of(datasetScope), detail);
  }

  private static String render(
      CompiledMapping mapping, Optional<MappingDatasetScope> datasetScope, Detail detail) {
    Objects.requireNonNull(mapping, "mapping");
    Objects.requireNonNull(datasetScope, "datasetScope");
    Objects.requireNonNull(detail, "detail");

    StringBuilder out = new StringBuilder();
    out.append("Mapping: ").append(mapping.name()).append('\n');
    out.append("View: ")
        .append(datasetScope.isPresent() ? "dataset" : "master schema")
        .append(" / ")
        .append(detail.name().toLowerCase())
        .append('\n');
    out.append("Core: ")
        .append(mapping.coreType())
        .append(" <- ")
        .append(mapping.coreSourceResource())
        .append('\n');

    renderScope(
        out,
        "CORE " + mapping.coreType(),
        collectCoreTargets(mapping),
        mergeIndex(mapping.coreTargetMerges()),
        decisionIndex(mapping.coreDecisions()),
        ownerRelations(mapping.coreFragments()),
        datasetScope,
        detail,
        null);

    for (CompiledExtension extension : mapping.extensions()) {
      Map<String, List<CompiledTargetProducer>> extensionTargets = collectExtensionTargets(extension);
      if (!hasVisibleTargets(extensionTargets, datasetScope)) {
        continue;
      }
      renderScope(
          out,
          "EXTENSION " + extension.rowType(),
          extensionTargets,
          mergeIndex(extension.targetMerges()),
          decisionIndex(extension.decisions()),
          ownerRelations(extension.fragments()),
          datasetScope,
          detail,
          extension);
    }

    return out.toString();
  }

  private static void renderScope(
      StringBuilder out,
      String heading,
      Map<String, List<CompiledTargetProducer>> targets,
      Map<String, CompiledTargetMerge> merges,
      Map<String, MappingDecision> decisions,
      Map<String, List<CompiledRelationStep>> relationsByOwner,
      Optional<MappingDatasetScope> datasetScope,
      Detail detail,
      CompiledExtension extension) {
    List<String> visibleTargets =
        targets.entrySet().stream()
            .filter(entry -> hasVisibleProducer(entry.getValue(), datasetScope))
            .map(Map.Entry::getKey)
            .sorted()
            .toList();

    out.append("\n").append(heading).append('\n');
    if (extension != null) {
      out.append("  rows: ").append(extension.rowComposition());
      extension.maxRowsPerParent().ifPresent(limit -> out.append("; max/parent=").append(limit));
      out.append('\n');
    }
    if (datasetScope.isPresent()) {
      out.append("  targets available: ")
          .append(visibleTargets.size())
          .append('/')
          .append(targets.size())
          .append('\n');
    }

    for (String target : visibleTargets) {
      List<CompiledTargetProducer> producers =
          targets.get(target).stream()
              .filter(producer -> datasetScope.map(scope -> scope.supports(producer)).orElse(true))
              .toList();
      renderTarget(
          out,
          target,
          producers,
          Optional.ofNullable(merges.get(target)),
          Optional.ofNullable(decisions.get(target)),
          relationsByOwner,
          datasetScope,
          detail);
    }
  }

  private static void renderTarget(
      StringBuilder out,
      String target,
      List<CompiledTargetProducer> producers,
      Optional<CompiledTargetMerge> merge,
      Optional<MappingDecision> decision,
      Map<String, List<CompiledRelationStep>> relationsByOwner,
      Optional<MappingDatasetScope> datasetScope,
      Detail detail) {
    out.append("\n  Target: ").append(target).append('\n');
    merge.ifPresent(value -> out.append("    merge: ").append(formatAggregation(value.aggregation())).append('\n'));

    if (detail == Detail.COMPACT) {
      for (CompiledTargetProducer producer : producers) {
        List<CompiledSourceField> visibleSources = visibleSources(producer, datasetScope);
        out.append("    <- ");
        if (visibleSources.size() > 1) {
          out.append(producer.sourceMode()).append(' ').append(formatAggregation(producer.aggregation())).append(' ');
        }
        out.append(
            visibleSources.stream()
                .map(CompiledSourceField::describe)
                .collect(Collectors.joining(" | ")));
        appendCompactPathSemantics(out, relationsByOwner.getOrDefault(producer.owner(), List.of()));
        out.append('\n');
      }
      return;
    }

    decision.ifPresent(
        value -> {
          out.append("    decision: ").append(value.type()).append('\n');
          out.append("      ").append(value.explanation()).append('\n');
        });

    for (CompiledTargetProducer producer : producers) {
      out.append("    Producer: ")
          .append(producer.owner())
          .append(" [")
          .append(producer.origin())
          .append("]\n");
      out.append("      values: ")
          .append(producer.sourceMode())
          .append(" / ")
          .append(formatAggregation(producer.aggregation()))
          .append('\n');
      if (producer.origin() == TargetFieldMapping.Origin.INFERRED) {
        out.append("      inferred depth: ").append(producer.pathDepth()).append('\n');
      }
      producer.contributionIdentity().ifPresent(
          source -> out.append("      contribution identity: ").append(source.describe()).append('\n'));
      producer.orderBy().ifPresent(
          source -> out.append("      order by: ").append(source.describe()).append('\n'));

      List<CompiledRelationStep> relations = relationsByOwner.getOrDefault(producer.owner(), List.of());
      if (!relations.isEmpty()) {
        out.append("      path:\n");
        for (CompiledRelationStep relation : relations) {
          out.append("        - ").append(relationDescription(relation)).append('\n');
        }
      }

      out.append("      sources:\n");
      for (CompiledSourceField source : visibleSources(producer, datasetScope)) {
        out.append("        - ").append(source.describe()).append('\n');
      }
    }
  }

  private static void appendCompactPathSemantics(
      StringBuilder out, List<CompiledRelationStep> relations) {
    if (relations.isEmpty()) {
      return;
    }
    out.append("  [");
    for (int i = 0; i < relations.size(); i++) {
      if (i > 0) {
        out.append(" -> ");
      }
      CompiledRelationStep relation = relations.get(i);
      out.append(relation.relation().targetResource());
      relation.cardinalityStrategy().ifPresent(strategy -> out.append(':').append(formatCardinality(strategy)));
      if (relation.filter().isPresent()) {
        out.append(":filter");
      }
    }
    out.append(']');
  }

  private static String relationDescription(CompiledRelationStep relation) {
    SchemaRelation schema = relation.relation();
    StringBuilder out = new StringBuilder();
    out.append(schema.sourceResource())
        .append('.')
        .append(schema.sourceColumn())
        .append(" -> ")
        .append(schema.targetResource())
        .append('.')
        .append(schema.targetColumn());
    out.append(relation.explicitColumns() ? " [EXPLICIT RELATION]" : " [SCHEMA RELATION]");
    if (schema.weak()) {
      out.append(" [WEAK]");
    }
    out.append(" [").append(relation.requirement()).append(']');
    relation.cardinalityStrategy().ifPresent(strategy -> out.append(" [").append(formatCardinality(strategy)).append(']'));
    schema.predicate().ifPresent(predicate -> out.append(" [predicate=").append(predicate).append(']'));
    if (relation.filter().isPresent()) {
      out.append(" [filter=Spark expression]");
    }
    return out.toString();
  }

  private static List<CompiledSourceField> visibleSources(
      CompiledTargetProducer producer, Optional<MappingDatasetScope> datasetScope) {
    return producer.sources().stream()
        .filter(source -> datasetScope.map(scope -> scope.supports(source)).orElse(true))
        .toList();
  }

  private static boolean hasVisibleProducer(
      List<CompiledTargetProducer> producers, Optional<MappingDatasetScope> datasetScope) {
    return datasetScope.isEmpty()
        || producers.stream().anyMatch(datasetScope.orElseThrow()::supports);
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
        extension.fragments().stream()
            .flatMap(fragment -> fragment.targets().stream())
            .toList());
  }

  private static Map<String, List<CompiledTargetProducer>> groupTargets(
      List<CompiledTargetProducer> producers) {
    return producers.stream()
        .collect(
            Collectors.groupingBy(
                CompiledTargetProducer::targetTerm,
                LinkedHashMap::new,
                Collectors.toList()));
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
    return targets.values().stream().anyMatch(producers -> hasVisibleProducer(producers, datasetScope));
  }

  private static String formatCardinality(CardinalityStrategy strategy) {
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

  private static String formatAggregation(ValueAggregation aggregation) {
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
