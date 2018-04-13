package org.gbif.pipelines.assembling.factory;

import org.gbif.pipelines.assembling.pipelines.GbifInterpretationPipeline;
import org.gbif.pipelines.assembling.pipelines.InterpretationPipeline;
import org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier;
import org.gbif.pipelines.assembling.pipelines.PipelinePaths;
import org.gbif.pipelines.assembling.utils.HdfsUtils;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.InterpretationType;

import java.util.EnumMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier.commonGbif;
import static org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier.locationGbif;
import static org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier.taxonomyGbif;
import static org.gbif.pipelines.assembling.pipelines.InterpretationStepSupplier.temporalGbif;

public class GbifPipelineFactory extends BasePipelineFactory {

  private static final String DATA_FILENAME = "interpreted";
  private static final String ISSUES_FOLDER = "issues";
  private static final String ISSUES_FILENAME = "issues";

  private static final Logger LOG = LoggerFactory.getLogger(GbifPipelineFactory.class);

  private final EnumMap<InterpretationType, InterpretationStepSupplier> stepsMap =
    new EnumMap<>(InterpretationType.class);

  private final DataProcessingPipelineOptions options;

  private GbifPipelineFactory(DataProcessingPipelineOptions options) {
    this.options = options;
    initStepsMap();
  }

  private void initStepsMap() {
    stepsMap.put(InterpretationType.LOCATION, locationGbif(createPipelinePaths(options, InterpretationType.LOCATION)));
    stepsMap.put(InterpretationType.TEMPORAL, temporalGbif(createPipelinePaths(options, InterpretationType.TEMPORAL)));
    stepsMap.put(InterpretationType.TAXONOMY, taxonomyGbif(createPipelinePaths(options, InterpretationType.TAXONOMY)));
    stepsMap.put(InterpretationType.COMMON, commonGbif(createPipelinePaths(options, InterpretationType.COMMON)));
  }

  public static GbifPipelineFactory newInstance(DataProcessingPipelineOptions options) {
    return new GbifPipelineFactory(options);
  }

  @Override
  InterpretationPipeline getInterpretationPipeline() {
    return GbifInterpretationPipeline.newInstance(options);
  }

  @Override
  EnumMap<InterpretationType, InterpretationStepSupplier> getStepsMap() {
    return stepsMap;
  }

  private static PipelinePaths createPipelinePaths(
    DataProcessingPipelineOptions options, InterpretationType interpretationType
  ) {
    PipelinePaths paths = new PipelinePaths();
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
