package org.gbif.pipelines.spark.util;

import static org.gbif.pipelines.spark.ArgsConstants.*;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.Arrays;
import java.util.stream.Stream;

@Parameters(separators = "=")
public class PipelineArgs {

  @Parameter(names = APP_NAME_ARG, description = "Application name", required = false)
  public String appName;

  @Parameter(names = DATASET_ID_ARG, description = "Dataset ID", required = false)
  public String datasetId;

  @Parameter(names = ATTEMPT_ID_ARG, description = "Attempt number", required = false)
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

  public static String[] addArgument(String[] args, String name, String value) {
    return Stream.concat(Arrays.stream(args), Arrays.stream(new String[] {name, value}))
        .toArray(String[]::new);
  }

  public static String[] addFlag(String[] args, String name) {
    return Stream.concat(Arrays.stream(args), Arrays.stream(new String[] {name}))
        .toArray(String[]::new);
  }
}
