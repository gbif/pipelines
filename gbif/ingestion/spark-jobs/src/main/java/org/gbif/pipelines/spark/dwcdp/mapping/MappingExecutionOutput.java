package org.gbif.pipelines.spark.dwcdp.mapping;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** ExtendedRecord output together with relation-branch execution diagnostics gathered while planning it. */
public record MappingExecutionOutput(
    Dataset<ExtendedRecord> records, List<MappingBranchExecutionMetrics> branchMetrics) {

  public MappingExecutionOutput {
    branchMetrics = List.copyOf(branchMetrics);
  }
}
