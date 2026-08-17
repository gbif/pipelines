package org.gbif.pipelines.spark.dwcdp.mapping.engine;

import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.SparkExtendedRecordExecutor;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingTraceRenderer;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingDatasetScope;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.TargetMappingPlanRenderer;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.TargetMappingPlanRenderer.Detail;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Runtime boundary for declarative DwC-DP -> DwC-A mappings.
 *
 * <p>The engine first compiles configuration against the selected DwC-DP schema. The resulting
 * {@link CompiledMapping} is engine-neutral and inspectable; Spark execution consumes that compiled
 * form rather than re-deriving source identity from raw column names.
 */
public final class DwcDpMappingEngine {
  private final SchemaGraph schemaGraph;
  private final MappingCompiler compiler;
  private final SparkExtendedRecordExecutor executor;

  public DwcDpMappingEngine(SchemaGraph schemaGraph) {
    this.schemaGraph = Objects.requireNonNull(schemaGraph, "schemaGraph");
    this.compiler = new MappingCompiler(schemaGraph);
    this.executor = new SparkExtendedRecordExecutor(schemaGraph);
  }

  /** Engine backed by the classpath bundle marked {@code isLatest=true}. */
  public static DwcDpMappingEngine currentSchema() {
    return new DwcDpMappingEngine(new DwcDpSchemaLoader().current());
  }

  public SchemaGraph schemaGraph() {
    return schemaGraph;
  }

  /** Returns the schema-resolved governing mapping without executing Spark. */
  public CompiledMapping compile(MappingPlan plan) {
    return compiler.compile(Objects.requireNonNull(plan, "plan"));
  }

  /** Human-readable full mapping trace, including all configured branches and schema paths. */
  public String trace(MappingPlan plan) {
    return MappingTraceRenderer.render(compile(plan));
  }

  /** Target-first compact view across the complete official schema. */
  public String targetPlan(MappingPlan plan) {
    return TargetMappingPlanRenderer.render(compile(plan), Detail.COMPACT);
  }

  /** Target-first detailed view across the complete official schema. */
  public String targetPlanDetailed(MappingPlan plan) {
    return TargetMappingPlanRenderer.render(compile(plan), Detail.DETAILED);
  }

  /** Target-first compact view pruned to resources and fields declared by one datapackage.json. */
  public String targetPlan(MappingPlan plan, DataPackage dataPackage) {
    return TargetMappingPlanRenderer.render(
        compile(plan), MappingDatasetScope.from(dataPackage), Detail.COMPACT);
  }

  /** Target-first detailed view pruned to resources and fields declared by one datapackage.json. */
  public String targetPlanDetailed(MappingPlan plan, DataPackage dataPackage) {
    return TargetMappingPlanRenderer.render(
        compile(plan), MappingDatasetScope.from(dataPackage), Detail.DETAILED);
  }

  public Dataset<ExtendedRecord> execute(TableLoader loader, MappingPlan plan) {
    Objects.requireNonNull(loader, "loader");
    return executor.execute(loader, compile(plan));
  }
}
