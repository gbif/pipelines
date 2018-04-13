package org.gbif.pipelines.assembling.pipelines;

import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

/**
 * Assembles a {@link Pipeline} dynamically that performs interpretations of verbatim data.
 */
public class InterpretationPipelineAssembler
  implements InterpretationPipelineBuilderSteps.WithOptionsStep, InterpretationPipelineBuilderSteps.WithInputStep,
  InterpretationPipelineBuilderSteps.UsingStep, InterpretationPipelineBuilderSteps.FinalStep {

  private static final String READ_STEP = "Read Avro files";

  private PipelineOptions options;
  private String input;
  private List<InterpretationType> interpretationTypes;

  private BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>> beforeHandler;
  private BiConsumer<PCollection<ExtendedRecord>, Pipeline> otherOperationsHandler;
  private Function<List<InterpretationType>, List<InterpretationStep>> stepsCreator;

  private InterpretationPipelineAssembler(List<InterpretationType> interpretationTypes) {
    this.interpretationTypes = interpretationTypes;
  }

  public static InterpretationPipelineBuilderSteps.WithOptionsStep of(List<InterpretationType> types) {
    Objects.requireNonNull(types);
    Preconditions.checkArgument(!types.isEmpty(), "Interpretation types are required");
    return new InterpretationPipelineAssembler(types);
  }

  @Override
  public InterpretationPipelineBuilderSteps.WithInputStep withOptions(PipelineOptions options) {
    Objects.requireNonNull(options);
    this.options = options;
    return this;
  }

  @Override
  public InterpretationPipelineBuilderSteps.UsingStep withInput(String input) {
    Objects.requireNonNull(input);
    this.input = input;
    return this;
  }

  @Override
  public InterpretationPipelineBuilderSteps.FinalStep using(Function<List<InterpretationType>, List<InterpretationStep>> stepsCreator) {
    Objects.requireNonNull(stepsCreator);
    this.stepsCreator = stepsCreator;
    return this;
  }

  @Override
  public InterpretationPipelineBuilderSteps.FinalStep onBeforeInterpretations(BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>> beforeHandler) {
    this.beforeHandler = beforeHandler;
    return this;
  }

  @Override
  public InterpretationPipelineBuilderSteps.FinalStep onOtherOperations(
    BiConsumer<PCollection<ExtendedRecord>, Pipeline> otherOperationsHandler
  ) {
    this.otherOperationsHandler = otherOperationsHandler;
    return this;
  }

  /**
   * Assembles a {@link Pipeline} from the parameters received.
   */
  @Override
  public Pipeline assemble() {
    // STEP 0: create pipeline from options
    Pipeline pipeline = Pipeline.create(options);

    // STEP 1: Read Avro files
    PCollection<ExtendedRecord> verbatimRecords =
      pipeline.apply(READ_STEP, AvroIO.read(ExtendedRecord.class).from(input));

    // STEP 2: Common operations before running the interpretations
    PCollection<ExtendedRecord> extendedRecords =
      Objects.nonNull(beforeHandler) ? beforeHandler.apply(verbatimRecords, pipeline) : verbatimRecords;

    // STEP 3: interpretations
    stepsCreator.apply(interpretationTypes).forEach(step -> step.appendToPipeline(extendedRecords, pipeline));

    // STEP 4: additional operations
    if (Objects.nonNull(otherOperationsHandler)) {
      otherOperationsHandler.accept(extendedRecords, pipeline);
    }

    return pipeline;
  }
}