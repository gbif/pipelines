package org.gbif.pipelines.assembling;

import org.apache.beam.sdk.Pipeline;
import org.gbif.pipelines.assembling.interpretation.GbifInterpretationPipeline;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;

import java.util.function.Supplier;

/**
 * Factory that creates {@link InterpretationPipeline} implementations.
 */
public class InterpretationPipelineFactory {

  private InterpretationPipelineFactory() {}

  /**
   * Creates a {@link InterpretationPipeline} from the parameters received.
   */
  public static Supplier<Pipeline> from(String[] args) {
    // currently we only support Gbif factory. To use more factories add the logic here.
    return GbifInterpretationPipeline.newInstance(DataPipelineOptionsFactory.create(args));
  }

}
