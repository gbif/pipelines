package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Options for piupelines that run against sampling services. */
public interface SamplingPipelineOptions extends AllDatasetsPipelinesOptions {

  @Description("Base URL for sampling service")
  @Default.String("https://sampling.ala.org.au/sampling-service/")
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

  @Description("Keep download sampling CSVs")
  @Default.Integer(5)
  Integer getDownloadRetries();

  void setDownloadRetries(Integer downloadRetries);

  @Description("Keep latlng export CSVs")
  @Default.Boolean(false)
  Boolean getKeepLatLngExports();

  void setKeepLatLngExports(Boolean keepLatLngExports);

  @Description("Keep download sampling CSVs")
  @Default.Boolean(false)
  Boolean getKeepSamplingDownloads();

  void setKeepSamplingDownloads(Boolean keepSamplingDownloads);

  @Description("Keep download sampling CSVs")
  @Default.Boolean(true)
  Boolean getDeleteSamplingForNewLayers();

  void setDeleteSamplingForNewLayers(Boolean deleteSamplingForNewLayers);
}
