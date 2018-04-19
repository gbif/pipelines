package org.gbif.pipelines.assembling.interpretation;

import org.apache.beam.sdk.Pipeline;

/**
 * Defines a factory to create a {@link Pipeline}.
 */
@FunctionalInterface
public interface InterpretationPipeline {

  /**
   * Creates a {@link Pipeline}.
   */
  Pipeline createPipeline();

}
