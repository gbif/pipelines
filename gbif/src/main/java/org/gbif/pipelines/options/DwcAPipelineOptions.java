package org.gbif.pipelines.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface DwcAPipelineOptions  extends PipelineOptions {

  @Description("Input DwCA file")
  @Validation.Required
  String getInputFile();
  void setInputFile(String inputFile);


  @Description("Destination path on where the output is stored")
  @Validation.Required
  String getOutputPath();
  void setOutputPath(String outputPath);
}
