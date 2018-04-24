package org.gbif.pipelines.assembling.interpretation;

import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier;
import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.EnumMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

/**
 * Defines the necessary interfaces to implement a step builder for {@link InterpretationPipelineAssembler}.
 */
class InterpretationAssemblerBuilderSteps {

  /**
   * Defines the step to add {@link PipelineOptions}.
   */
  public interface WithOptionsStep {

    WithInputStep withOptions(PipelineOptions options);
  }

  /**
   * Defines the step to the input.
   */
  public interface WithInputStep {

    UsingStep withInput(String input);
  }

  /**
   * Defines the step to add map of {@link InterpretationStepSupplier} per each {@link InterpretationType}.
   */
  public interface UsingStep {

    FinalStep using(EnumMap<InterpretationType, InterpretationStepSupplier> interpretationSteps);
  }

  /**
   * Defines the final step of the builder.
   */
  public interface FinalStep {

    Pipeline assemble();

    FinalStep onBeforeInterpretations(
      BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>> beforeHandler
    );

    FinalStep onOtherOperations(BiConsumer<PCollection<ExtendedRecord>, Pipeline> otherOperationsHandler);
  }

}
