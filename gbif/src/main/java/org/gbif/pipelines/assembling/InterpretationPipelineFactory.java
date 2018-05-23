package org.gbif.pipelines.assembling;

import org.gbif.pipelines.assembling.interpretation.GbifInterpretationPipeline;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;

import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;

/**
 * Factory that creates pipeline implementations.
 */
public class InterpretationPipelineFactory {

  private InterpretationPipelineFactory() {}

  /**
   * Creates a pipeline from the parameters received.
   */
  public static Supplier<Pipeline> from(String[] args) {
    // currently we only support Gbif factory. To use more factories add the logic here.
    return GbifInterpretationPipeline.of(DataPipelineOptionsFactory.create(args));
  }

}
