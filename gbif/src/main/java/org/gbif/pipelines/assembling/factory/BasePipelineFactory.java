package org.gbif.pipelines.assembling.factory;

import org.gbif.pipelines.assembling.pipelines.InterpretationStep;
import org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier;
import org.gbif.pipelines.config.InterpretationType;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class that implements a {@link PipelineFactory} and provides some methods that can be reused by multiple
 * factories.
 */
abstract class BasePipelineFactory implements PipelineFactory {

  /**
   * Creates a list of {@link InterpretationStep} for every {@link InterpretationType} received. The creation is
   * based on the {@link EnumMap<InterpretationType, InterpretationStepSupplier>}, which acts as a factory.
   *
   * @param types    list of {@link InterpretationStep} to iterate through
   * @param stepsMap this map contains a {@link InterpretationStepSupplier} for each {@link InterpretationType}.
   *
   * @return list of {@link InterpretationStep}
   */
  protected List<InterpretationStep> createInterpretationSteps(
    List<InterpretationType> types, EnumMap<InterpretationType, InterpretationStepSupplier> stepsMap
  ) {
    return types.stream().map(type -> stepsMap.get(type).get()).collect(Collectors.toList());
  }

  /**
   * Checks the list of {@link InterpretationType} to evaluate what interpretations should be performed.
   */
  protected List<InterpretationType> checkInterpretations(List<InterpretationType> types) {
    if (types == null || types.isEmpty() || types.contains(InterpretationType.ALL)) {
      // we return all interpretations
      return Arrays.asList(InterpretationType.values());
    }

    return types;
  }

}
