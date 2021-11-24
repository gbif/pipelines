package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/**
 * Options for pipelines that run against spatial service for Expert distribution layer calculation.
 */
public interface DistributionPipelineOptions extends AllDatasetsPipelinesOptions {

  @Description("Base URL for Spatial service")
  @Default.String("https://spatial.ala.org.au/ws/")
  String getBaseUrl();

  void setBaseUrl(String baseUrl);

  @Description("Default batch size")
  @Default.Integer(25000)
  Integer getBatchSize();

  void setBatchSize(Integer batchSize);

  @Description("Keep download sampling CSVs")
  @Default.Integer(1000)
  Integer getBatchStatusSleepTime();

  void setBatchStatusSleepTime(Integer batchStatusSleepTime);
}
