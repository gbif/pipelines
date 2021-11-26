package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/**
 * Options for pipelines that run against spatial service for Expert distribution layer calculation.
 */
public interface DistributionOutlierPipelineOptions extends AllDatasetsPipelinesOptions {

  @Description("Base URL for Spatial service")
  @Default.String("https://spatial.ala.org.au/ws/")
  String getBaseUrl();

  void setBaseUrl(String baseUrl);
}
