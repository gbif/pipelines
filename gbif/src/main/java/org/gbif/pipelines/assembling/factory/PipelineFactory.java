package org.gbif.pipelines.assembling.factory;

import org.gbif.pipelines.config.InterpretationType;

import java.util.List;

import org.apache.beam.sdk.Pipeline;

@FunctionalInterface
public interface PipelineFactory {

  Pipeline createPipeline(List<InterpretationType> interpretationTypes);

}
