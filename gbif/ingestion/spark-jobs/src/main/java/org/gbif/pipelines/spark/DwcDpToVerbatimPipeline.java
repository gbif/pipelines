package org.gbif.pipelines.spark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter.VerbatimConversionMetrics;
import org.gbif.pipelines.spark.util.PipelineArgs;
import org.gbif.pipelines.spark.util.PipelineRunner;
import org.gbif.pipelines.spark.util.PipelinesConfigUtil;

/**
 * Spark pipeline entry point that converts DwC-DP Parquet files into a verbatim.avro file
 * compatible with the existing GBIF interpretation pipeline.
 *
 * <p>Reads datapackage.json to discover which tables are present, then delegates all conversion
 * logic to {@link DwcDpVerbatimConverter}.
 *
 * <p>Output is written to {@code {inputPath}/{datasetId}/{attempt}/verbatim.avro}, the location
 * that IdentifiersPipeline and EventInterpretationPipeline read from.
 */
@Slf4j
public class DwcDpToVerbatimPipeline {

  @Parameters(separators = "=")
  static class Args extends PipelineArgs {

    @Parameter(names = "--containsEvents", arity = 1)
    boolean containsEvents = false;

    @Parameter(names = "--containsOccurrences", arity = 1)
    boolean containsOccurrences = false;
  }

  public static void main(String[] argsv) throws Exception {
    Args args = new Args();
    JCommander jCommander = new JCommander(args);
    jCommander.setAcceptUnknownOptions(true);
    jCommander.parse(argsv);

    if (args.help) {
      jCommander.usage();
      return;
    }

    run(args);
  }

  /** Entry point for CLI / Airflow — owns Spark session lifecycle. */
  public static void run(Args args) throws Exception {
    PipelinesConfig config = PipelinesConfigUtil.loadConfig(args.config);

    PipelineRunner.run(
        args,
        config,
        (builder, cfg) -> {},
        (spark, fileSystem) ->
            DwcDpVerbatimConverter.convert(
                spark,
                fileSystem,
                config,
                args.datasetId,
                args.attempt,
                args.containsEvents,
                args.containsOccurrences));
  }

  /**
   * Entry point for callbacks — Spark session and FileSystem already initialised by {@link
   * PipelinesCallback}. Mirrors the pattern of {@link DataPackageConversionPipeline#runCopy}.
   *
   * @return record counts gathered during conversion, for the callback to forward in the outgoing
   *     message's validation result / event and occurrence counts.
   */
  public static VerbatimConversionMetrics run(
      SparkSession spark,
      FileSystem fileSystem,
      PipelinesConfig config,
      String datasetId,
      int attempt,
      boolean containsEvents,
      boolean containsOccurrences)
      throws Exception {
    return DwcDpVerbatimConverter.convert(
        spark, fileSystem, config, datasetId, attempt, containsEvents, containsOccurrences);
  }
}
