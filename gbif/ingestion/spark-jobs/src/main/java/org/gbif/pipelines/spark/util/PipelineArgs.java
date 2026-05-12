package org.gbif.pipelines.spark.util;

import static org.gbif.pipelines.spark.ArgsConstants.*;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@Parameters(separators = "=")
public class PipelineArgs {

  @Parameter(names = APP_NAME_ARG, description = "Application name", required = true)
  public String appName;

  @Parameter(names = DATASET_ID_ARG, description = "Dataset ID", required = true)
  public String datasetId;

  @Parameter(names = ATTEMPT_ID_ARG, description = "Attempt number", required = true)
  public int attempt;

  @Parameter(names = CONFIG_PATH_ARG, description = "Path to YAML configuration file")
  public String config = "/tmp/pipelines-spark.yaml";

  @Parameter(names = SPARK_MASTER_ARG, description = "Spark master - local dev only")
  public String master;

  @Parameter(
      names = {"--help", "-h"},
      help = true,
      description = "Show usage")
  public boolean help;

  @Parameter(names = "--useSystemExit", arity = 1)
  public boolean useSystemExit = true;
}
