package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

/** Human-readable renderer for governing/review/debug traces. */
public final class MappingTraceRenderer {
  private MappingTraceRenderer() {}

  public static String render(CompiledMapping mapping) {
    StringBuilder out = new StringBuilder();
    out.append("Mapping: ").append(mapping.name()).append('\n');
    out.append("Core: ").append(mapping.coreType()).append(" <- ").append(mapping.coreSourceResource()).append('\n');

    if (!mapping.coreTargets().isEmpty()) {
      out.append("\nCore targets:\n");
      for (CompiledTargetProducer target : mapping.coreTargets()) {
        renderTarget(out, target, "  ");
      }
    }

    if (!mapping.coreDecisions().isEmpty()) {
      out.append("\nCore decisions:\n");
      for (MappingDecision decision : mapping.coreDecisions()) {
        renderDecision(out, decision, "  ");
      }
    }

    for (CompiledExtension extension : mapping.extensions()) {
      out.append("\nExtension: ").append(extension.rowType()).append('\n');
      if (!extension.decisions().isEmpty()) {
        out.append("  Decisions:\n");
        for (MappingDecision decision : extension.decisions()) {
          renderDecision(out, decision, "    ");
        }
      }
      for (CompiledFragment fragment : extension.fragments()) {
        out.append("  Fragment: ").append(fragment.name()).append('\n');
        out.append("    Source: ").append(fragment.sourceResource()).append('\n');
        if (!fragment.relations().isEmpty()) {
          out.append("    Path:\n");
          for (CompiledRelationStep relation : fragment.relations()) {
            out.append("      - ").append(relation.describe()).append('\n');
          }
        }
        fragment.rowIdentity().ifPresent(
            identity -> out.append("    Row identity: ").append(identity.qualifiedName()).append('\n'));
        for (CompiledTargetProducer target : fragment.targets()) {
          renderTarget(out, target, "    ");
        }
      }
    }
    return out.toString();
  }

  private static void renderDecision(StringBuilder out, MappingDecision decision, String indent) {
    out.append(indent).append(decision.type()).append(": ").append(decision.targetTerm()).append('\n');
    out.append(indent).append("  ").append(decision.explanation()).append('\n');
    decision.selected().ifPresent(
        selected -> out.append(indent).append("  Selected: ").append(selected.owner())
            .append(" [").append(selected.origin()).append("]\n"));
    if (decision.candidates().size() > 1) {
      out.append(indent).append("  Candidates:\n");
      for (CompiledTargetProducer candidate : decision.candidates()) {
        out.append(indent).append("    - ").append(candidate.owner())
            .append(" [").append(candidate.origin()).append("]");
        if (candidate.origin() == org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping.Origin.INFERRED) {
          out.append(" depth=").append(candidate.pathDepth());
        }
        out.append('\n');
      }
    }
  }

  private static void renderTarget(StringBuilder out, CompiledTargetProducer target, String indent) {
    out.append(indent).append("Target: ").append(target.targetTerm()).append('\n');
    out.append(indent).append("  Strategy: ").append(target.sourceMode()).append(" / ")
        .append(target.aggregation()).append('\n');
    out.append(indent).append("  Sources:\n");
    for (CompiledSourceField source : target.sources()) {
      out.append(indent).append("    - ").append(source.describe()).append('\n');
    }
  }
}
