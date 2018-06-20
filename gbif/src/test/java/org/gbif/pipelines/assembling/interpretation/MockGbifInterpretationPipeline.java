package org.gbif.pipelines.assembling.interpretation;

import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.core.ws.config.Config;

import java.util.EnumMap;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;

import static org.gbif.pipelines.assembling.interpretation.GbifInterpretationPipeline.createPaths;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.commonGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.locationGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.multimediaGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.taxonomyGbif;
import static org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier.temporalGbif;
import static org.gbif.pipelines.config.InterpretationType.COMMON;
import static org.gbif.pipelines.config.InterpretationType.LOCATION;
import static org.gbif.pipelines.config.InterpretationType.MULTIMEDIA;
import static org.gbif.pipelines.config.InterpretationType.TAXONOMY;
import static org.gbif.pipelines.config.InterpretationType.TEMPORAL;

/** Creates mock interpretation pipelines to make them easier to test. */
public final class MockGbifInterpretationPipeline {

  private MockGbifInterpretationPipeline() {}

  /**
   * Creates an interpretation {@link Pipeline} from {@link DataProcessingPipelineOptions} and
   * {@link Config}.
   */
  public static Pipeline mockInterpretationPipeline(
      DataProcessingPipelineOptions options, Config wsConfig) {
    return InterpretationPipelineAssembler.of(options.getInterpretationTypes())
        .withOptions(options)
        .withInput(options.getInputFile())
        .using(createStepsMap(options, wsConfig))
        .assemble();
  }

  private static EnumMap<InterpretationType, InterpretationStepSupplier> createStepsMap(
      DataProcessingPipelineOptions options, Config wsConfig) {
    EnumMap<InterpretationType, InterpretationStepSupplier> stepsMap =
        new EnumMap<>(InterpretationType.class);
    CodecFactory avroCodec = CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);

    stepsMap.put(
        LOCATION,
        locationGbif(createPaths(options, InterpretationType.LOCATION), avroCodec, wsConfig));
    stepsMap.put(
        TEMPORAL, temporalGbif(createPaths(options, InterpretationType.TEMPORAL), avroCodec));
    stepsMap.put(
        TAXONOMY,
        taxonomyGbif(createPaths(options, InterpretationType.TAXONOMY), avroCodec, wsConfig));
    stepsMap.put(COMMON, commonGbif(createPaths(options, InterpretationType.COMMON), avroCodec));
    stepsMap.put(
        MULTIMEDIA, multimediaGbif(createPaths(options, InterpretationType.MULTIMEDIA), avroCodec));

    return stepsMap;
  }
}
