package org.gbif.pipelines.spark.dwcdp.mapping.engine;

import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.mapping.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.ExecutionMetricsCollector;
import org.gbif.pipelines.spark.dwcdp.mapping.MappingExecutionOutput;
import org.gbif.pipelines.spark.dwcdp.mapping.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.ProjectedTableLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.SparkExtendedRecordExecutor;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingInputRequirements;
import org.gbif.pipelines.spark.dwcdp.mapping.compiled.MappingInputRequirementsAnalyzer;
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

  public DwcDpMappingEngine(SchemaGraph schemaGraph) {
    this.schemaGraph = Objects.requireNonNull(schemaGraph, "schemaGraph");
    this.compiler = new MappingCompiler(schemaGraph);
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

  /** Physical resources and columns required by the compiled canonical plan. */
  public MappingInputRequirements inputRequirements(MappingPlan plan) {
    return new MappingInputRequirementsAnalyzer(schemaGraph).analyze(compile(plan));
  }

  public Dataset<ExtendedRecord> execute(TableLoader loader, MappingPlan plan) {
    return executeWithMetrics(loader, plan).records();
  }

  /** Executes the mapping and exposes the relation-branch diagnostics already gathered by the Spark path executor. */
  public MappingExecutionOutput executeWithMetrics(TableLoader loader, MappingPlan plan) {
    Objects.requireNonNull(loader, "loader");
    CompiledMapping compiled = compile(plan);
    MappingInputRequirements requirements =
        new MappingInputRequirementsAnalyzer(schemaGraph).analyze(compiled);
    TableLoader projectedLoader = ProjectedTableLoader.wrap(loader, requirements);

    ExecutionMetricsCollector collector = new ExecutionMetricsCollector();
    SparkExtendedRecordExecutor executor = new SparkExtendedRecordExecutor(schemaGraph, collector);
    Dataset<ExtendedRecord> records = executor.execute(projectedLoader, compiled);
    return new MappingExecutionOutput(records, collector.snapshot());
  }
}
