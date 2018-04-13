package org.gbif.pipelines.assembling.pipelines;

import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

/**
 * Defines the necessary interfaces to implement a step builder for {@link InterpretationPipelineAssembler}.
 */
class InterpretationPipelineBuilderSteps {

  public interface WithOptionsStep {

    WithInputStep withOptions(PipelineOptions options);
  }

  public interface WithInputStep {

    UsingStep withInput(String input);
  }

  public interface UsingStep {

    FinalStep using(Function<List<InterpretationType>, List<InterpretationStep>> stepsCreator);
  }

  public interface FinalStep {

    Pipeline assemble();

    FinalStep onBeforeInterpretations(
      BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>> beforeHandler
    );

    FinalStep onOtherOperations(BiConsumer<PCollection<ExtendedRecord>, Pipeline> otherOperationsHandler);
  }

}
