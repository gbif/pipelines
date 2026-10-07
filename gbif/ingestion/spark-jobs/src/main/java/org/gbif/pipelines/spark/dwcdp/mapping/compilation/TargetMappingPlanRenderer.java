package org.gbif.pipelines.spark.dwcdp.mapping.compilation;

import java.util.Optional;

/** Human-readable renderer for the structured target-first mapping plan report. */
public final class TargetMappingPlanRenderer {

  public enum Detail {
    COMPACT,
    DETAILED
  }

  private TargetMappingPlanRenderer() {}

  public static String render(CompiledMapping mapping, Detail detail) {
    return render(TargetMappingPlanReport.from(mapping, Optional.empty(), detail));
  }

  public static String render(
      CompiledMapping mapping, MappingDatasetScope datasetScope, Detail detail) {
    return render(TargetMappingPlanReport.from(mapping, Optional.of(datasetScope), detail));
  }

  public static String render(TargetMappingPlanReport report) {
    StringBuilder out = new StringBuilder();
    out.append("Mapping: ").append(report.mapping()).append('\n');
    out.append("View: ").append(report.view()).append(" / ").append(report.detail()).append('\n');
    out.append("Core: ")
        .append(report.coreType())
        .append(" <- ")
        .append(report.coreSourceResource())
        .append('\n');

    boolean detailed = Detail.DETAILED.name().equalsIgnoreCase(report.detail());
    for (TargetMappingPlanReport.Scope scope : report.scopes()) {
      out.append("\n").append(scope.kind()).append(' ').append(scope.name()).append('\n');
      if (scope.rowComposition() != null) {
        out.append("  rows: ").append(scope.rowComposition());
        if (scope.maxRowsPerParent() != null) {
          out.append("; max/parent=").append(scope.maxRowsPerParent());
        }
        out.append('\n');
      }
      if (scope.targetsAvailable() != null) {
        out.append("  targets available: ")
            .append(scope.targetsAvailable())
            .append('/')
            .append(scope.targetsTotal())
            .append('\n');
      }

      for (TargetMappingPlanReport.Target target : scope.targets()) {
        out.append("\n  Target: ").append(target.term()).append('\n');
        if (target.merge() != null) {
          out.append("    merge: ").append(target.merge()).append('\n');
        }

        if (!detailed) {
          for (TargetMappingPlanReport.Producer producer : target.producers()) {
            out.append("    <- ");
            if ("EXPRESSION".equals(producer.valueType())) {
              out.append("EXPRESSION ");
            } else if (producer.sources().size() > 1) {
              out.append(producer.sourceMode())
                  .append(' ')
                  .append(producer.aggregation())
                  .append(' ');
            }
            out.append(String.join(" | ", producer.sources()));
            appendCompactPathSemantics(out, producer.path());
            out.append('\n');
          }
          continue;
        }

        if (target.decision() != null) {
          out.append("    decision: ").append(target.decision().type()).append('\n');
          out.append("      ").append(target.decision().explanation()).append('\n');
        }

        for (TargetMappingPlanReport.Producer producer : target.producers()) {
          out.append("    Producer: ")
              .append(producer.owner())
              .append(" [")
              .append(producer.origin())
              .append("]\n");
          out.append("      values: ");
          if ("EXPRESSION".equals(producer.valueType())) {
            out.append("EXPRESSION / ").append(producer.expression());
          } else {
            out.append(producer.sourceMode()).append(" / ").append(producer.aggregation());
          }
          out.append('\n');
          if (producer.inferredDepth() != null) {
            out.append("      inferred depth: ").append(producer.inferredDepth()).append('\n');
          }
          if (producer.contributionIdentity() != null) {
            out.append("      contribution identity: ")
                .append(producer.contributionIdentity())
                .append('\n');
          }
          if (producer.orderBy() != null) {
            out.append("      order by: ").append(producer.orderBy()).append('\n');
          }
          if (!producer.path().isEmpty()) {
            out.append("      path:\n");
            for (TargetMappingPlanReport.PathStep relation : producer.path()) {
              out.append("        - ").append(relationDescription(relation)).append('\n');
            }
          }
          out.append("      sources:\n");
          for (String source : producer.sources()) {
            out.append("        - ").append(source).append('\n');
          }
        }
      }
    }
    return out.toString();
  }

  private static void appendCompactPathSemantics(
      StringBuilder out, java.util.List<TargetMappingPlanReport.PathStep> relations) {
    if (relations.isEmpty()) {
      return;
    }
    out.append("  [");
    for (int i = 0; i < relations.size(); i++) {
      if (i > 0) {
        out.append(" -> ");
      }
      TargetMappingPlanReport.PathStep relation = relations.get(i);
      out.append(relation.targetResource());
      if (relation.cardinality() != null) {
        out.append(':').append(relation.cardinality());
      }
      if (relation.filtered()) {
        out.append(":filter");
      }
    }
    out.append(']');
  }

  private static String relationDescription(TargetMappingPlanReport.PathStep relation) {
    StringBuilder out = new StringBuilder();
    out.append(relation.sourceResource())
        .append('.')
        .append(relation.sourceColumn())
        .append(" -> ")
        .append(relation.targetResource())
        .append('.')
        .append(relation.targetColumn());
    out.append(
        "EXPLICIT".equals(relation.relationType()) ? " [EXPLICIT RELATION]" : " [SCHEMA RELATION]");
    if (relation.weak()) {
      out.append(" [WEAK]");
    }
    out.append(" [").append(relation.requirement()).append(']');
    if (relation.cardinality() != null) {
      out.append(" [").append(relation.cardinality()).append(']');
    }
    if (relation.predicate() != null) {
      out.append(" [predicate=").append(relation.predicate()).append(']');
    }
    if (relation.filtered()) {
      out.append(" [filter=Spark expression]");
    }
    return out.toString();
  }
}
