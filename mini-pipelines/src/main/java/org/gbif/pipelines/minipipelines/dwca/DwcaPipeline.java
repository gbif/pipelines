package org.gbif.pipelines.minipipelines.dwca;

import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** Entry point to run a pipeline that works wiht Dwc-A files. */
public class DwcaPipeline {

  public static void main(String[] args) {
    // Create PipelineOptions
    PipelineOptionsFactory.register(DwcaPipelineOptions.class);
    DwcaPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DwcaPipelineOptions.class);

    DwcaPipelineRunner.from(options).run();
  }
}
