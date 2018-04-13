package org.gbif.pipelines.assembling.pipelines;

import java.util.List;

import org.apache.beam.sdk.Pipeline;

@FunctionalInterface
public interface InterpretationPipeline {

  Pipeline createInterpretationPipeline(List<InterpretationStep> steps);

}
