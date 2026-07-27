package org.gbif.pipelines.spark.util;

import static org.gbif.pipelines.spark.ArgsConstants.*;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/** Arguments for pipelines that operate on a single dataset/attempt. */
@Parameters(separators = "=")
public class SingleDatasetPipelineArgs extends PipelineArgs {

  @Parameter(names = DATASET_ID_ARG, description = "Dataset ID", required = true)
  public String datasetId;

  @Parameter(names = ATTEMPT_ID_ARG, description = "Attempt number", required = true)
  public int attempt;
}
