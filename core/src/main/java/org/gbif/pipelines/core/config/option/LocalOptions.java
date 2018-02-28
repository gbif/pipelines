package org.gbif.pipelines.core.config.option;

import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;

import java.io.File;

import org.apache.beam.sdk.options.PipelineOptions;

public class LocalOptions implements Options {

  @Override
  public String createDefaultDirectoryFactory(PipelineOptions options) {
    return System.getProperty("user.home")
           + File.separator
           + "gbif-data"
           + File.separator
           + ((DataProcessingPipelineOptions) options).getDatasetId();
  }

  @Override
  public String createTempDirectoryFactory(PipelineOptions options) {
    return System.getProperty("user.home")
           + File.separator
           + "gbif-data"
           + File.separator
           + ((DataProcessingPipelineOptions) options).getDatasetId()
           + File.separator
           + "temp";
  }

}
