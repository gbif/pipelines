package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * ExtendedRecord output together with relation-branch execution diagnostics gathered while planning
 * it.
 *
 * <p>When shared path-prefix caching is enabled, callers that execute actions on {@link #records()}
 * should close this output afterwards so persisted shared intermediates are released.
 */
public record MappingExecutionOutput(
    Dataset<ExtendedRecord> records,
    List<MappingBranchExecutionMetrics> branchMetrics,
    Runnable releaseAction)
    implements AutoCloseable {

  public MappingExecutionOutput {
    Objects.requireNonNull(records, "records");
    branchMetrics = List.copyOf(branchMetrics);
    releaseAction = releaseAction == null ? () -> {} : releaseAction;
  }

  public MappingExecutionOutput(
      Dataset<ExtendedRecord> records, List<MappingBranchExecutionMetrics> branchMetrics) {
    this(records, branchMetrics, () -> {});
  }

  @Override
  public void close() {
    releaseAction.run();
  }
}
