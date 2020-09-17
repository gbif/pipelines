package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.ingest.options.InterpretationPipelineOptions;

public interface ImageServicePipelineOptions extends InterpretationPipelineOptions {

  @Description("Image Service Url")
  @Default.String("https://images-dev.ala.org.au")
  String getImageServiceUrl();

  void setImageServiceUrl(String imageServiceUrl);
}
