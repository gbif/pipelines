package org.gbif.pipelines.assembling;

import org.gbif.pipelines.assembling.interpretation.GbifInterpretationPipeline;
import org.gbif.pipelines.assembling.interpretation.InterpretationPipeline;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;

import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Factory that creates {@link InterpretationPipeline} implementations.
 */
public class InterpretationPipelineFactory {

  /**
   * Creates a {@link InterpretationPipeline} from the parameters received.
   */
  public static InterpretationPipeline from(String[] args) {
    // currently we only support Gbif factory. To use more factories add the logic here.
    return GbifInterpretationPipeline.newInstance(PipelineOptionsFactory.fromArgs(args)
                                                    .as(DataProcessingPipelineOptions.class));
  }

}
