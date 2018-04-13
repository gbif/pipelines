package org.gbif.pipelines.assembling;

import org.gbif.pipelines.assembling.factory.PipelineAbstractFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a pipeline dynamically and run it after. The creation of the pipeline is delegates to other classes.
 * <p>
 * This class is intended to be run from the command line.
 */
public class PipelineManager {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineManager.class);

  public static void main(String[] args) {
    createAndRunPipeline(args);
  }

  private static PipelineResult.State createAndRunPipeline(String[] args) {

    Pipeline pipeline = PipelineAbstractFactory.from(args).createPipeline();

    PipelineResult.State state = pipeline.run().waitUntilFinish();

    LOG.info("Pipeline finished with state {}", state.toString());

    return state;
  }

}
