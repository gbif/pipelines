package org.gbif.pipelines.demo.utils;

import org.gbif.pipelines.core.config.DataFlowPipelineOptions;
import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.config.RecordInterpretation;
import org.gbif.pipelines.core.config.TargetPath;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public final class PipelineUtils {

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
    targetPaths.put(RecordInterpretation.GBIF_BACKBONE,
                    new TargetPath(taxonOutPath, RecordInterpretation.GBIF_BACKBONE.getDefaultFileName()));
    targetPaths.put(RecordInterpretation.ISSUES,
                    new TargetPath(issuesOutPath, RecordInterpretation.ISSUES.getDefaultFileName()));
    options.setTargetPaths(targetPaths);

    return options;
  }

  /**
   * create data flow pipeline options from arguments
   */
  public static DataFlowPipelineOptions createPipelineOptions(String[] args) {
    DataFlowPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(DataFlowPipelineOptions.class);
    if (options.getHDFSConfigurationDirectory() != null) {
      options.setHdfsConfiguration(getHadoopConfiguration(options.getHDFSConfigurationDirectory()));
    }
    return options;
  }

  /**
   * gets the configuration from the list of xmls in the hadoop client configuration
   */
  private static List<Configuration> getHadoopConfiguration(String configurationDirectory) {
    Configuration config = new Configuration();
    File file = new File(configurationDirectory);
    if (file.isFile()) {
      //all xml files in one directory
      if (file.isDirectory()) {
        final List<File> collect = Arrays.stream(file.listFiles())
          .filter(f -> f.isFile() && f.getName().endsWith(".xml"))
          .collect(Collectors.toList());
        for (File filteredFile : collect) {
          config.addResource(new Path(filteredFile.getAbsolutePath()));
        }
      } else {
        //only one xml file
        config.addResource(new Path(file.getAbsolutePath()));
      }
    }
    return Collections.singletonList(config);
  }

  private PipelineUtils() {}

}
