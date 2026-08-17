package org.gbif.pipelines.spark.dwcdp.mapping.engine;

import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkPathPrefixCache;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.DwcDpSchemaLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.definition.MappingPlan;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.ExecutionMetricsCollector;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.MappingExecutionOutput;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.ProjectedTableLoader;
import org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkExtendedRecordExecutor;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.CompiledMappingDatasetPruner;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingCompiler;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingInputRequirements;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingInputRequirementsAnalyzer;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingTraceRenderer;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.MappingDatasetScope;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.TargetMappingPlanRenderer;
import org.gbif.pipelines.spark.dwcdp.mapping.compilation.TargetMappingPlanRenderer.Detail;
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

  /**
   * Returns the canonical compiled mapping pruned to resources and columns declared by one dataset.
   * Canonical producer precedence is resolved before dataset pruning.
   */
  public CompiledMapping compile(MappingPlan plan, DataPackage dataPackage) {
    CompiledMapping compiled = compile(plan);
    return new CompiledMappingDatasetPruner()
        .prune(compiled, MappingDatasetScope.from(Objects.requireNonNull(dataPackage, "dataPackage")));
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
        compile(plan, dataPackage), MappingDatasetScope.from(dataPackage), Detail.COMPACT);
  }

  /** Target-first detailed view pruned to resources and fields declared by one datapackage.json. */
  public String targetPlanDetailed(MappingPlan plan, DataPackage dataPackage) {
    return TargetMappingPlanRenderer.render(
        compile(plan, dataPackage), MappingDatasetScope.from(dataPackage), Detail.DETAILED);
  }

  /** Physical resources and columns required by the compiled canonical plan. */
  public MappingInputRequirements inputRequirements(MappingPlan plan) {
    return new MappingInputRequirementsAnalyzer(schemaGraph).analyze(compile(plan));
  }

  /** Physical resources and columns required after pruning the canonical plan to one dataset. */
  public MappingInputRequirements inputRequirements(MappingPlan plan, DataPackage dataPackage) {
    MappingDatasetScope scope = MappingDatasetScope.from(dataPackage);
    return new MappingInputRequirementsAnalyzer(schemaGraph, scope)
        .analyze(new CompiledMappingDatasetPruner().prune(compile(plan), scope));
  }

  public Dataset<ExtendedRecord> execute(TableLoader loader, MappingPlan plan) {
    return executeCompiled(loader, compile(plan), null, false).records();
  }

  /** Executes the canonical mapping after pruning it to one datapackage.json. */
  public Dataset<ExtendedRecord> execute(
      TableLoader loader, MappingPlan plan, DataPackage dataPackage) {
    MappingDatasetScope scope = MappingDatasetScope.from(dataPackage);
    CompiledMapping compiled = new CompiledMappingDatasetPruner().prune(compile(plan), scope);
    return executeCompiled(loader, compiled, scope, false).records();
  }

  /** Executes the mapping and exposes the relation-branch diagnostics already gathered by the Spark path executor. */
  public MappingExecutionOutput executeWithMetrics(TableLoader loader, MappingPlan plan) {
    return executeCompiled(loader, compile(plan));
  }

  /** Executes the dataset-pruned canonical mapping and exposes its relation-branch diagnostics. */
  public MappingExecutionOutput executeWithMetrics(
      TableLoader loader, MappingPlan plan, DataPackage dataPackage) {
    MappingDatasetScope scope = MappingDatasetScope.from(dataPackage);
    CompiledMapping compiled = new CompiledMappingDatasetPruner().prune(compile(plan), scope);
    return executeCompiled(loader, compiled, scope, true);
  }

  private MappingExecutionOutput executeCompiled(TableLoader loader, CompiledMapping compiled) {
    return executeCompiled(loader, compiled, null, true);
  }

  private MappingExecutionOutput executeCompiled(
      TableLoader loader,
      CompiledMapping compiled,
      MappingDatasetScope datasetScope,
      boolean sharePathPrefixes) {
    Objects.requireNonNull(loader, "loader");
    MappingInputRequirementsAnalyzer analyzer =
        datasetScope == null
            ? new MappingInputRequirementsAnalyzer(schemaGraph)
            : new MappingInputRequirementsAnalyzer(schemaGraph, datasetScope);
    MappingInputRequirements requirements = analyzer.analyze(compiled);
    TableLoader projectedLoader = ProjectedTableLoader.wrap(loader, requirements);

    ExecutionMetricsCollector collector = new ExecutionMetricsCollector();
    org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkPathPrefixCache prefixCache =
        sharePathPrefixes
            ? org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkPathPrefixCache.enabled()
            : org.gbif.pipelines.spark.dwcdp.mapping.execution.SparkPathPrefixCache.disabled();
    SparkExtendedRecordExecutor executor =
        new SparkExtendedRecordExecutor(schemaGraph, collector, prefixCache);
    Dataset<ExtendedRecord> records = executor.execute(projectedLoader, compiled);
    return new MappingExecutionOutput(records, collector.snapshot(), prefixCache::close);
  }
}
