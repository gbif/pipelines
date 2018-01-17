package org.gbif.pipelines.demo.utils;

import org.gbif.pipelines.core.config.HdfsExporterOptions;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;

public final class PipelineUtils {

  private PipelineUtils() {}

  /**
   * Creates a {@link HdfsExporterOptions} from the arguments and configuration passed.
   *
   * @param args   cli args
   * @param config hadoop config
   *
   * @return {@link HdfsExporterOptions}
   */
  public static HdfsExporterOptions createPipelineOptions(Configuration config, String[] args) {
    HdfsExporterOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(HdfsExporterOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  public static HdfsExporterOptions createPipelineOptions(Configuration config) {
    HdfsExporterOptions options = PipelineOptionsFactory.as(HdfsExporterOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  /**
   * Generates a target path from {@link HdfsExporterOptions} by concatenating the target directory and the file name.
   *
   * @param options {@link HdfsExporterOptions} with the target directory and the file name
   *
   * @return target path generated
   */
  public static String targetPath(HdfsExporterOptions options) {
    String targetDir = Optional.ofNullable(options.getTargetDirectory())
      .filter(s -> !s.isEmpty())
      .orElseThrow(() -> new IllegalArgumentException("missing targetDirectory argument"));
    String datasetId = Optional.ofNullable(options.getDatasetId())
      .filter(s -> !s.isEmpty())
      .orElseThrow(() -> new IllegalArgumentException("missing datasetId argument"));

    return targetDir.endsWith(File.separator) ? targetDir + datasetId : targetDir + File.separator + datasetId;
  }

}
