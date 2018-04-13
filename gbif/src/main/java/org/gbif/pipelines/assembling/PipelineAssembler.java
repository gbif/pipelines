package org.gbif.pipelines.assembling;

import org.gbif.pipelines.assembling.factory.GbifPipelineFactory;
import org.gbif.pipelines.assembling.factory.PipelineFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineAssembler {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineAssembler.class);

  public static void main(String[] args) {
    createAndRunPipeline(args);
  }

  private static PipelineResult.State createAndRunPipeline(String[] args) {

    PipelineFactory pipelineFactory = getPipelineFactory(args);

    Pipeline pipeline = pipelineFactory.createPipeline();

    PipelineResult.State state = pipeline.run().waitUntilFinish();

    LOG.info("Pipeline finished with state {}", state.toString());

    return state;
  }

  private static PipelineFactory getPipelineFactory(String[] args) {
    // currenlty we only use Gbif factory. To use more factories add the logic here.
    return GbifPipelineFactory.newInstance(PipelineOptionsFactory.fromArgs(args)
                                             .as(DataProcessingPipelineOptions.class));
  }

}
