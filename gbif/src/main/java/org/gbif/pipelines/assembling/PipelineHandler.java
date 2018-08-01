package org.gbif.pipelines.assembling;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.assembling.GbifInterpretationType.ALL;
import static org.gbif.pipelines.assembling.GbifInterpretationType.METADATA;

/**
 * Creates a pipeline dynamically and run it after. The creation of the pipeline is delegated to
 * other classes.
 *
 * <p>This class is intended to be run from the command line.
 */
public class PipelineHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineHandler.class);

  /**
   * Main method that receives the command line arguments and invokes the method to create and run
   * the {@link Pipeline}.
   */
  public static void main(String[] args) {
    createAndRunPipeline(args);
  }

  private static void createAndRunPipeline(String[] args) {
    LOG.info("Creating pipeline from args: {}", Arrays.asList(args));
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);

    LOG.info("Running pipeline", Arrays.asList(args));

    // Create metadata avro
    List<String> types = options.getInterpretationTypes();
    if (types.contains(ALL.name()) || types.contains(METADATA.name())) {
      MetadataPipeline.run(options);
    }

    // Create temporal/common/location/etc. avros
    Pipeline pipeline = GbifInterpretationPipeline.create(options).get();
    PipelineResult.State state = pipeline.run().waitUntilFinish();

    LOG.info("Pipeline finished with state {} from args: {}", state, args);
  }
}
