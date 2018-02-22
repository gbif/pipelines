package org.gbif.pipelines.demo.utils;

import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.config.RecordInterpretation;
import org.gbif.pipelines.core.config.TargetPath;

import java.util.Collections;
import java.util.EnumMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;

public final class PipelineUtils {

  private PipelineUtils() {}

  /**
   * Creates a {@link DataProcessingPipelineOptions} from the arguments and configuration passed.
   *
   * @param args   cli args
   * @param config hadoop config
   *
   * @return {@link DataProcessingPipelineOptions}
   */
  public static DataProcessingPipelineOptions createPipelineOptions(Configuration config, String[] args) {
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  public static DataProcessingPipelineOptions createPipelineOptions(Configuration config) {
    DataProcessingPipelineOptions options = PipelineOptionsFactory.as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  public static DataProcessingPipelineOptions createPipelineOptionsFromArgsWithoutValidation(
    Configuration config, String[] args
  ) {
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  /**
   * Creates a PipelineOptions suitable to interpret taxonomic records in HDFS.
   */
  @VisibleForTesting
  public static DataProcessingPipelineOptions createDefaultTaxonOptions(
    Configuration config, String sourcePath, String taxonOutPath, String issuesOutPath, String[] args
  ) {
    // create options
    DataProcessingPipelineOptions options = PipelineUtils.createPipelineOptionsFromArgsWithoutValidation(config, args);
    options.setInputFile(sourcePath);

    // target paths
    EnumMap<RecordInterpretation, TargetPath> targetPaths = new EnumMap<>(RecordInterpretation.class);
    targetPaths.put(RecordInterpretation.TAXONOMY,
                    new TargetPath(taxonOutPath, RecordInterpretation.TAXONOMY.getDefaultFileName()));
    targetPaths.put(RecordInterpretation.ISSUES, new TargetPath(issuesOutPath, RecordInterpretation.ISSUES.getDefaultFileName()));
    options.setTargetPaths(targetPaths);

    return options;
  }

}
