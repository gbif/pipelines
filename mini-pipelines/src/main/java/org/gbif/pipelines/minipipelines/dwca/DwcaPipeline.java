package org.gbif.pipelines.minipipelines.dwca;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point to run a pipeline that works wiht Dwc-A files. */
public class DwcaPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaPipeline.class);

  public static void main(String[] args) {
    LOG.info("Creating DwcaMiniPipelineOptions");
    // Create PipelineOptions
    PipelineOptionsFactory.register(DwcaMiniPipelineOptions.class);
    DwcaMiniPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DwcaMiniPipelineOptions.class);

    LOG.info("Running Dwc-A pipeline");
    DwcaPipelineRunner.from(options).run();
  }
}
