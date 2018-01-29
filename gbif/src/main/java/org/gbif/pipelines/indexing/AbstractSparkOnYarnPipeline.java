package org.gbif.pipelines.indexing;

import java.util.Collections;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;

@Deprecated
public abstract class AbstractSparkOnYarnPipeline {

  /**
   * Instanciates the pipeline.
   * @param args Provided arguments
   * @return An HDFS configured pipeline suitable for Spark submission
   */
  static Pipeline newPipeline(String args[], Configuration conf) {
    HadoopFileSystemOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(conf));
    return Pipeline.create(options);
  }
}
