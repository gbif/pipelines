package org.gbif.pipelines.config;

import java.util.Collections;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.hadoop.conf.Configuration;

public final class DataPipelineOptionsFactory {

  private DataPipelineOptionsFactory() {
    // Can't have an instance
  }

  /**
   * Creates a {@link DataProcessingPipelineOptions} from the arguments and configuration passed.
   *
   * @param args   cli args
   * @param config hadoop config
   *
   * @return {@link DataProcessingPipelineOptions}
   */
  public static DataProcessingPipelineOptions create(Configuration config, String[] args) {
    PipelineOptionsFactory.register(DataProcessingPipelineOptions.class);
    DataProcessingPipelineOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

  public static DataProcessingPipelineOptions create(String[] args) {
    return create(new Configuration(), args);
  }

  public static DataProcessingPipelineOptions create(Configuration config) {
    PipelineOptionsFactory.register(DataProcessingPipelineOptions.class);
    DataProcessingPipelineOptions options = PipelineOptionsFactory.as(DataProcessingPipelineOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));

    return options;
  }

}
