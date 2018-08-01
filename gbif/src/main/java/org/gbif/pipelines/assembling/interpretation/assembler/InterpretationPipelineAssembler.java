package org.gbif.pipelines.assembling.interpretation.assembler;

import org.gbif.pipelines.assembling.GbifInterpretationType;
import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStep;
import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assembles a {@link Pipeline} dynamically that performs interpretations of verbatim data. */
public class InterpretationPipelineAssembler
    implements InterpretationAssemblerBuilderSteps.WithOptionsStep,
        InterpretationAssemblerBuilderSteps.WithInputStep,
        InterpretationAssemblerBuilderSteps.UsingStep,
        InterpretationAssemblerBuilderSteps.FinalStep {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretationPipelineAssembler.class);

  private PipelineOptions options;
  private String input;
  private Set<String> interpretationTypes;
  private BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>>
      beforeHandler;
  private BiConsumer<PCollection<ExtendedRecord>, Pipeline> otherOperationsHandler;
  private Map<String, InterpretationStepSupplier> interpretationSteps;

  private InterpretationPipelineAssembler(Set<String> interpretationTypes) {
    this.interpretationTypes = interpretationTypes;
  }

  /** Creates a {@link InterpretationPipelineAssembler} for the list of {@link String} received. */
  public static InterpretationAssemblerBuilderSteps.WithOptionsStep of(
      List<String> interpretationTypes) {
    return new InterpretationPipelineAssembler(filterInterpretations(interpretationTypes));
  }

  /**
   * Filters the interpretations received.
   *
   * <p>By default, we use all the interpretations in case that we receive a null or empty list of
   * {@link String}.
   */
  private static Set<String> filterInterpretations(List<String> types) {
    return Objects.isNull(types)
            || types.isEmpty()
            || types.contains(GbifInterpretationType.ALL.name())
        ? new TreeSet<>(Collections.singletonList(GbifInterpretationType.ALL.name()))
        : new TreeSet<>(types);
  }

  @Override
  public InterpretationAssemblerBuilderSteps.WithInputStep withOptions(PipelineOptions options) {
    Objects.requireNonNull(options, "PipelineOptions cannot be null");
    this.options = options;
    return this;
  }

  @Override
  public InterpretationAssemblerBuilderSteps.UsingStep withInput(String input) {
    Objects.requireNonNull(input, "Input cannot be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(input), "Input cannot be empty");
    this.input = input;
    return this;
  }

  @Override
  public InterpretationAssemblerBuilderSteps.FinalStep using(
      Map<String, InterpretationStepSupplier> interpretationSteps) {
    Objects.requireNonNull(interpretationSteps, "Interpretation steps map cannot be null");
    this.interpretationSteps = interpretationSteps;
    return this;
  }

  @Override
  public InterpretationAssemblerBuilderSteps.FinalStep onBeforeInterpretations(
      BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>>
          beforeHandler) {
    this.beforeHandler = beforeHandler;
    return this;
  }

  @Override
  public InterpretationAssemblerBuilderSteps.FinalStep onOtherOperations(
      BiConsumer<PCollection<ExtendedRecord>, Pipeline> otherOperationsHandler) {
    this.otherOperationsHandler = otherOperationsHandler;
    return this;
  }

  /** Assembles a {@link Pipeline} from the parameters received. */
  @Override
  public Pipeline assemble() {
    // STEP 0: create pipeline from options
    LOG.info("Creating pipeline from options");
    Pipeline pipeline = Pipeline.create(options);

    // STEP 1: Read Avro files
    LOG.info("Reading Avro records from {}", input);
    PCollection<ExtendedRecord> verbatimRecords =
        pipeline.apply("Read Avro files", AvroIO.read(ExtendedRecord.class).from(input));

    // STEP 2: Common operations before running the interpretations
    LOG.info("Steps before interpretation - {}", Objects.nonNull(beforeHandler));
    PCollection<ExtendedRecord> extendedRecords =
        Objects.nonNull(beforeHandler)
            ? beforeHandler.apply(verbatimRecords, pipeline)
            : verbatimRecords;

    // STEP 3: interpretations
    LOG.info("Adding interpretation steps");
    if (interpretationTypes.contains(GbifInterpretationType.ALL.name())) {
      interpretationTypes = interpretationSteps.keySet();
    }

    interpretationTypes
        .stream()
        .filter(stepSupplierFilter())
        .map(interpretationStepMapper())
        .filter(Objects::nonNull)
        .forEach(step -> step.appendToPipeline(extendedRecords, pipeline));

    // STEP 4: additional operations
    if (Objects.nonNull(otherOperationsHandler)) {
      LOG.info("Adding other operations to the pipeline");
      otherOperationsHandler.accept(extendedRecords, pipeline);
    }

    return pipeline;
  }

  private Predicate<String> stepSupplierFilter() {
    return type -> {
      if (Objects.isNull(interpretationSteps.get(type))) {
        LOG.warn("No interpretation step supplier found for interpretation type {}", type);
        return false;
      }
      return true;
    };
  }

  private Function<String, InterpretationStep> interpretationStepMapper() {
    return type -> {
      InterpretationStep step = interpretationSteps.get(type).get();
      if (step == null) {
        LOG.warn("No interpretation step found for interpretation type {}", type);
      }
      return step;
    };
  }
}
