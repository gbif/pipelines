package org.gbif.pipelines.assembling;

import org.gbif.pipelines.assembling.interpretation.assembler.InterpretationPipelineAssembler;
import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import static org.gbif.pipelines.assembling.GbifInterpretationType.COMMON;
import static org.gbif.pipelines.assembling.GbifInterpretationType.LOCATION;
import static org.gbif.pipelines.assembling.GbifInterpretationType.MULTIMEDIA;
import static org.gbif.pipelines.assembling.GbifInterpretationType.TAXONOMY;
import static org.gbif.pipelines.assembling.GbifInterpretationType.TEMPORAL;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.commonGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.locationGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.multimediaGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.taxonomyGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.temporalGbif;
import static org.gbif.pipelines.utils.CodecUtils.parseAvroCodec;
import static org.gbif.pipelines.utils.FsUtils.createPaths;

/** Gbif implementation for a pipeline. */
public class GbifInterpretationPipeline implements Supplier<Pipeline> {

  private Map<String, InterpretationStepSupplier> stepsMap = new TreeMap<>();
  private final DataProcessingPipelineOptions options;
  private final CodecFactory avroCodec;
  private final String wsProperties;

  private GbifInterpretationPipeline(DataProcessingPipelineOptions options) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getDatasetId()), "datasetId is required");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(options.getDefaultTargetDirectory()),
        "defaultTargetDirectory is required");
    Preconditions.checkArgument(Objects.nonNull(options.getAttempt()), "attempt is required");
    this.options = options;
    avroCodec = parseAvroCodec(options.getAvroCompressionType());
    // we don't create the Config here because it's only required for taxon and location
    // interpretations. For the
    // rest, it's not even required to set the ws properties in the PipelineOptions.
    wsProperties = options.getWsProperties();
    initStepsMap();
  }

  /**
   * Creates a new {@link GbifInterpretationPipeline} instance from the {@link
   * DataProcessingPipelineOptions} received.
   */
  public static GbifInterpretationPipeline create(DataProcessingPipelineOptions options) {
    return new GbifInterpretationPipeline(options);
  }

  @Override
  public Pipeline get() {
    return InterpretationPipelineAssembler.of(options.getInterpretationTypes())
        .withOptions(options)
        .withInput(options.getInputFile())
        .using(stepsMap)
        .onBeforeInterpretations(createBeforeStep())
        .assemble();
  }

  private void initStepsMap() {
    stepsMap.put(COMMON.name(), commonGbif(createPaths(options, COMMON.name()), avroCodec));
    stepsMap.put(TEMPORAL.name(), temporalGbif(createPaths(options, TEMPORAL.name()), avroCodec));
    stepsMap.put(
        MULTIMEDIA.name(), multimediaGbif(createPaths(options, MULTIMEDIA.name()), avroCodec));
    stepsMap.put(
        LOCATION.name(),
        locationGbif(createPaths(options, LOCATION.name()), avroCodec, wsProperties));
    stepsMap.put(
        TAXONOMY.name(),
        taxonomyGbif(createPaths(options, TAXONOMY.name()), avroCodec, wsProperties));
  }

  public BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>>
      createBeforeStep() {
    return (PCollection<ExtendedRecord> verbatimRecords, Pipeline pipeline) -> {
      UniqueOccurrenceIdTransform uniquenessTransform =
          UniqueOccurrenceIdTransform.create().withAvroCoders(pipeline);
      PCollectionTuple uniqueTuple = verbatimRecords.apply(uniquenessTransform);
      return uniqueTuple.get(uniquenessTransform.getDataTag());
    };
  }

  public Map<String, InterpretationStepSupplier> getStepsMap() {
    return stepsMap;
  }

  public InterpretationStepSupplier addNewStep(String stepName, InterpretationStepSupplier step) {
    return stepsMap.put(stepName, step);
  }

  public InterpretationStepSupplier removeStep(String stepName) {
    return stepsMap.remove(stepName);
  }

  public CodecFactory getAvroCodec() {
    return avroCodec;
  }
}
