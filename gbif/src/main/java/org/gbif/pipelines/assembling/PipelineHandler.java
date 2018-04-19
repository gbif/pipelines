package org.gbif.pipelines.assembling;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a pipeline dynamically and run it after. The creation of the pipeline is delegated to other classes.
 * <p>
 * This class is intended to be run from the command line.
 */
public class PipelineHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineHandler.class);

  /**
   * Main method that receives the command line arguments and invokes the method to create and run the {@link Pipeline}.
   */
  public static void main(String[] args) {
    createAndRunPipeline(args);
  }

  private static void createAndRunPipeline(String[] args) {
    Pipeline pipeline = InterpretationPipelineFactory.from(args).createPipeline();
    LOG.info("Pipeline created from args: {}", args);

    PipelineResult.State state = pipeline.run().waitUntilFinish();
    LOG.info("Pipeline finished with state {} from args: {}", state, args);
  }

}
