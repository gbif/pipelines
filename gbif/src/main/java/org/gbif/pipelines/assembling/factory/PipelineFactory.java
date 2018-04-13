package org.gbif.pipelines.assembling.factory;

import org.apache.beam.sdk.Pipeline;

/**
 * Defines a factory to create a {@link Pipeline}.
 */
@FunctionalInterface
public interface PipelineFactory {

  Pipeline createPipeline();

}
