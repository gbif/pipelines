package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Options for piupelines that run against sampling services. */
public interface SamplingPipelineOptions extends AllDatasetsPipelinesOptions {

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
