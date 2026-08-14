package org.gbif.pipelines.spark.dwcdp.mapping.compiled;

import java.util.List;

/** Raised when the governing mapping contains one or more problems that prevent compilation. */
public final class MappingCompilationException extends IllegalArgumentException {
  private final List<MappingDecision> problems;

  public MappingCompilationException(List<MappingDecision> problems) {
    super(render(problems));
    this.problems = List.copyOf(problems);
  }

  public List<MappingDecision> problems() {
    return problems;
  }

  private static String render(List<MappingDecision> problems) {
    StringBuilder out = new StringBuilder("Mapping compilation failed with unresolved problems:\n");
    for (MappingDecision problem : problems) {
      out.append("\nScope: ").append(problem.scope()).append('\n');
      out.append("Target: ").append(problem.targetTerm()).append('\n');
      out.append("Decision: ").append(problem.type()).append('\n');
      out.append("Reason: ").append(problem.explanation()).append('\n');
      out.append("Candidates:\n");
      for (CompiledTargetProducer candidate : problem.candidates()) {
        out.append("  - ").append(candidate.owner())
            .append(" [").append(candidate.origin()).append("]");
        if (candidate.origin() == org.gbif.pipelines.spark.dwcdp.mapping.TargetFieldMapping.Origin.INFERRED) {
          out.append(" depth=").append(candidate.pathDepth());
        }
        out.append('\n');
        for (CompiledSourceField source : candidate.sources()) {
          out.append("      ").append(source.describe()).append('\n');
        }
      }
    }
    return out.toString();
  }
}
