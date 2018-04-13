package org.gbif.pipelines.assembling.factory;

import org.gbif.pipelines.config.DataProcessingPipelineOptions;

import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Abstract factory that creates {@link PipelineFactory} implementations.
 */
public class PipelineAbstractFactory {

  /**
   * Creates a {@link PipelineFactory} from the parameters received.
   */
  public static PipelineFactory from(String[] args) {
    // currenlty we only support Gbif factory. To use more factories add the logic here.
    return GbifPipelineFactory.newInstance(PipelineOptionsFactory.fromArgs(args)
                                             .as(DataProcessingPipelineOptions.class));
  }

}
