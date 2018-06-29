package org.gbif.pipelines.assembling.interpretation;

import org.gbif.pipelines.assembling.GbifInterpretationType;
import org.gbif.pipelines.assembling.interpretation.assembler.InterpretationPipelineAssembler;
import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.ws.config.Config;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.sdk.Pipeline;

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
import static org.gbif.pipelines.utils.FsUtils.createPaths;

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

  private static Map<String, InterpretationStepSupplier> createStepsMap(
      DataProcessingPipelineOptions options, Config wsConfig) {
    Map<String, InterpretationStepSupplier> stepsMap = new HashMap<>();
    CodecFactory avroCodec = CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);

    stepsMap.put(
        LOCATION.name(),
        locationGbif(
          createPaths(options, GbifInterpretationType.LOCATION.name()), avroCodec, wsConfig));
    stepsMap.put(
        TEMPORAL.name(),
        temporalGbif(createPaths(options, GbifInterpretationType.TEMPORAL.name()), avroCodec));
    stepsMap.put(
        TAXONOMY.name(),
        taxonomyGbif(
            createPaths(options, GbifInterpretationType.TAXONOMY.name()), avroCodec, wsConfig));
    stepsMap.put(
        COMMON.name(),
        commonGbif(createPaths(options, GbifInterpretationType.COMMON.name()), avroCodec));
    stepsMap.put(
        MULTIMEDIA.name(),
        multimediaGbif(createPaths(options, GbifInterpretationType.MULTIMEDIA.name()), avroCodec));

    return stepsMap;
  }
}
