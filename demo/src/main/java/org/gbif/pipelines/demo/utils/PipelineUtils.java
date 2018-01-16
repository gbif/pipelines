package org.gbif.pipelines.demo.utils;

import java.util.Collections;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;

public final class PipelineUtils {

  /**
   * Instanciates the pipeline.
   *
   * @param args Provided arguments
   *
   * @return An HDFS configured pipeline
   */
  public static Pipeline newHadoopPipeline(String args[], Configuration conf) {
    HadoopFileSystemOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().withoutStrictParsing().as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(conf));
    return Pipeline.create(options);
  }

  /**
   * Gets the targetPath argument from the command line args.
   *
   * @param args from command line
   *
   * @return the target path
   */
  public static String getTargetPath(String[] args) {
    String targetPath = ArgsParser.getArgShortForm(args, Constants.TARGET_PATH_ARG_NAME, true);

    // we add timestamp to the target path in order to run the same pipeline with the same path several times
    return targetPath.concat(String.valueOf(System.currentTimeMillis()));
  }

  /**
   * Generates the temp path created from the root of the hdfs.
   *
   * @param config hadoop configuration
   *
   * @return path of the temp directory
   */
  public static String tmpPath(Configuration config) {
    return config.get("fs.defaultFS").concat(Constants.TEMP_DIR_HDFS);
  }

}
