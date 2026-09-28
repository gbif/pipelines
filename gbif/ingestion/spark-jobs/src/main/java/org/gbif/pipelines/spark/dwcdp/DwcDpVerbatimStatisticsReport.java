package org.gbif.pipelines.spark.dwcdp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.MappingBranchExecutionMetrics;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.RelationExecutionMetrics;

/** Structured conversion statistics shared by the text and JSON report outputs. */
public record DwcDpVerbatimStatisticsReport(
    String datasetId, Map<String, Long> sourceTables, List<Branch> mappingBranches, Output output) {

  public DwcDpVerbatimStatisticsReport {
    sourceTables = Collections.unmodifiableMap(new LinkedHashMap<>(sourceTables));
    mappingBranches = List.copyOf(mappingBranches);
  }

  public record Branch(String name, List<Relation> relations) {
    public Branch {
      relations = List.copyOf(relations);
    }
  }

  public record Relation(
      String sourceResource,
      String targetResource,
      String cardinality,
      String requirement,
      boolean filtered,
      boolean skipped,
      long inputRows,
      long sourceKeyPresentRows,
      long matchedParentRows,
      long singleMatchParentRows,
      long multipleMatchParentRows,
      long unmatchedParentRows,
      long targetRowsBeforeFilter,
      long targetRowsAfterFilter,
      long outputRows) {}

  public record Output(
      long coreRecordsWritten, boolean outputDatasetSupplied, List<Extension> extensions) {
    public Output {
      extensions = List.copyOf(extensions);
    }
  }

  public record Extension(String rowType, long rows, long recordsWithThisExtension) {}

  public static List<Branch> branches(List<MappingBranchExecutionMetrics> metrics) {
    return metrics.stream()
        .map(
            branch ->
                new Branch(
                    branch.branchName(),
                    branch.relations().stream()
                        .map(DwcDpVerbatimStatisticsReport::relation)
                        .toList()))
        .toList();
  }

  private static Relation relation(RelationExecutionMetrics relation) {
    long singleMatch =
        Math.max(0L, relation.matchedParentRows() - relation.multipleMatchParentRows());
    return new Relation(
        relation.sourceResource(),
        relation.targetResource(),
        relation.cardinality(),
        relation.requirement(),
        relation.filtered(),
        relation.skipped(),
        relation.inputRows(),
        relation.sourceKeyPresentRows(),
        relation.matchedParentRows(),
        singleMatch,
        relation.multipleMatchParentRows(),
        relation.unmatchedParentRows(),
        relation.targetRowsBeforeFilter(),
        relation.targetRowsAfterFilter(),
        relation.outputRows());
  }

  public String renderText() {
    List<String> lines = new ArrayList<>();
    lines.add("DwC-DP conversion report: " + datasetId);
    lines.add("");
    lines.add("source tables (raw row counts):");
    sourceTables.forEach((resource, count) -> lines.add("  " + resource + ": " + count));

    lines.add("");
    lines.add("mapping branches (execution funnels):");
    if (mappingBranches.isEmpty()) {
      lines.add("  (execution metrics not supplied)");
    } else {
      for (Branch branch : mappingBranches) {
        lines.add("  " + branch.name());
        int relationNumber = 1;
        for (Relation relation : branch.relations()) {
          lines.add(
              "    "
                  + relationNumber++
                  + ". "
                  + relation.sourceResource()
                  + " -> "
                  + relation.targetResource()
                  + " ["
                  + relation.cardinality()
                  + ", "
                  + relation.requirement()
                  + (relation.filtered() ? ", FILTERED" : "")
                  + (relation.skipped() ? ", SKIPPED" : "")
                  + "]");
          lines.add(
              "       parents: input="
                  + relation.inputRows()
                  + ", key-present="
                  + relation.sourceKeyPresentRows()
                  + ", matched="
                  + relation.matchedParentRows()
                  + ", single-match="
                  + relation.singleMatchParentRows()
                  + ", multi-match="
                  + relation.multipleMatchParentRows()
                  + ", unmatched="
                  + relation.unmatchedParentRows());
          lines.add(
              "       target: before-filter="
                  + relation.targetRowsBeforeFilter()
                  + ", after-filter="
                  + relation.targetRowsAfterFilter()
                  + ", output-rows="
                  + relation.outputRows());
        }
      }
    }

    lines.add("");
    lines.add("output extensions (rows actually written):");
    lines.add("  core records written: " + output.coreRecordsWritten());
    if (!output.outputDatasetSupplied()) {
      lines.add("  (output dataset not supplied)");
    } else {
      for (Extension extension : output.extensions()) {
        lines.add(
            "  "
                + extension.rowType()
                + ": rows="
                + extension.rows()
                + ", records-with-this-ext="
                + extension.recordsWithThisExtension());
      }
    }

    return String.join("\n", lines) + "\n";
  }
}
