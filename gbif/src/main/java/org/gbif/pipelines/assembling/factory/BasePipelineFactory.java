package org.gbif.pipelines.assembling.factory;

import org.gbif.pipelines.assembling.pipelines.InterpretationPipeline;
import org.gbif.pipelines.assembling.pipelines.InterpretationStep;
import org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier;
import org.gbif.pipelines.config.InterpretationType;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;

abstract class BasePipelineFactory implements PipelineFactory {

  @Override
  public Pipeline createPipeline(List<InterpretationType> interpretationTypes) {
    return getInterpretationPipeline().createInterpretationPipeline(createInterpretationSteps(interpretationTypes));
  }

  abstract InterpretationPipeline getInterpretationPipeline();

  abstract EnumMap<InterpretationType, InterpretationStepSupplier> getStepsMap();

  private List<InterpretationStep> createInterpretationSteps(List<InterpretationType> types) {
    return processInterpretations(types).stream()
      .map(type -> getStepsMap().get(type).get())
      .collect(Collectors.toList());
  }

  private List<InterpretationType> processInterpretations(List<InterpretationType> types) {
    if (types == null || types.isEmpty() || types.contains(InterpretationType.ALL)) {
      // we return all interpretations
      return Arrays.asList(InterpretationType.values());
    }

    return types;
  }

}
