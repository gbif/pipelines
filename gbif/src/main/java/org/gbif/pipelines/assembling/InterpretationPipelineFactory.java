package org.gbif.pipelines.assembling;

import org.gbif.pipelines.assembling.interpretation.GbifInterpretationPipeline;
import org.gbif.pipelines.assembling.interpretation.InterpretationPipeline;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;

/**
 * Factory that creates {@link InterpretationPipeline} implementations.
 */
public class InterpretationPipelineFactory {

  private InterpretationPipelineFactory() {}

  /**
   * Creates a {@link InterpretationPipeline} from the parameters received.
   */
  public static InterpretationPipeline from(String[] args) {
    // currently we only support Gbif factory. To use more factories add the logic here.
    return GbifInterpretationPipeline.newInstance(DataPipelineOptionsFactory.create(args));
  }

}
