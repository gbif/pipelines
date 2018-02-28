package org.gbif.pipelines.core.config.option;

import org.apache.beam.sdk.options.PipelineOptions;

public interface Options {

  String createDefaultDirectoryFactory(PipelineOptions options);

  String createTempDirectoryFactory(PipelineOptions options);

}
