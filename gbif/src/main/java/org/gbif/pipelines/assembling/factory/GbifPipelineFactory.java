package org.gbif.pipelines.assembling.factory;

import org.gbif.pipelines.assembling.pipelines.InterpretationPipelineAssembler;
import org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier;
import org.gbif.pipelines.assembling.pipelines.PipelineTargetPaths;
import org.gbif.pipelines.assembling.utils.HdfsUtils;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.InterpretationType;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;

import java.util.EnumMap;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

import static org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier.commonGbif;
import static org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier.locationGbif;
import static org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier.taxonomyGbif;
import static org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier.temporalGbif;

/**
 * Gbif implementation for a {@link PipelineFactory}.
 */
public class GbifPipelineFactory extends BasePipelineFactory {

  private static final String DATA_FILENAME = "interpreted";
  private static final String ISSUES_FOLDER = "issues";
  private static final String ISSUES_FILENAME = "issues";

  private final EnumMap<InterpretationType, InterpretationStepSupplier> stepsMap =
    new EnumMap<>(InterpretationType.class);
  private final DataProcessingPipelineOptions options;

  private GbifPipelineFactory(DataProcessingPipelineOptions options) {
    this.options = options;
    initStepsMap();
  }

  static GbifPipelineFactory newInstance(DataProcessingPipelineOptions options) {
    return new GbifPipelineFactory(options);
  }

  private void initStepsMap() {
    stepsMap.put(InterpretationType.LOCATION, locationGbif(createTargetPaths(options, InterpretationType.LOCATION)));
    stepsMap.put(InterpretationType.TEMPORAL, temporalGbif(createTargetPaths(options, InterpretationType.TEMPORAL)));
    stepsMap.put(InterpretationType.TAXONOMY, taxonomyGbif(createTargetPaths(options, InterpretationType.TAXONOMY)));
    stepsMap.put(InterpretationType.COMMON, commonGbif(createTargetPaths(options, InterpretationType.COMMON)));
  }

  @Override
  public Pipeline createPipeline() {
    List<InterpretationType> interpretationTypes = checkInterpretations(options.getInterpretationTypes());

    return InterpretationPipelineAssembler.of(interpretationTypes)
      .withOptions(options)
      .withInput(options.getInputFile())
      .using((types) -> createInterpretationSteps(types, stepsMap))
      .onBeforeInterpretations(createBeforeStep())
      .assemble();
  }

  private BiFunction<PCollection<ExtendedRecord>, Pipeline, PCollection<ExtendedRecord>> createBeforeStep() {
    return (PCollection<ExtendedRecord> verbatimRecords, Pipeline pipeline) -> {
      UniqueOccurrenceIdTransform uniquenessTransform = new UniqueOccurrenceIdTransform().withAvroCoders(pipeline);
      PCollectionTuple uniqueTuple = verbatimRecords.apply(uniquenessTransform);
      return uniqueTuple.get(uniquenessTransform.getDataTag());
    };

  }

  private static PipelineTargetPaths createTargetPaths(
    DataProcessingPipelineOptions options, InterpretationType interpretationType
  ) {
    PipelineTargetPaths paths = new PipelineTargetPaths();
    paths.setDataTargetPath(HdfsUtils.getHdfsPath(options.getHdfsConfiguration().get(0),
                                                  options.getDefaultTargetDirectory(),
                                                  options.getDatasetId(),
                                                  interpretationType.name(),
                                                  DATA_FILENAME));

    paths.setIssuesTargetPath(HdfsUtils.getHdfsPath(options.getHdfsConfiguration().get(0),
                                                    options.getDefaultTargetDirectory(),
                                                    options.getDatasetId(),
                                                    interpretationType.name(),
                                                    ISSUES_FOLDER,
                                                    ISSUES_FILENAME));

    return paths;
  }

}
