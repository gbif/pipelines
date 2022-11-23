package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Main pipeline options necessary for DwCA export for Living atlases */
public interface DwCAExportPipelineOptions extends IndexingPipelineOptions {

  @Description("Message format compatible Image service URL path")
  String getImageServicePath();

  void setImageServicePath(String imageServicePath);

  @Description("Get local export path to write archives")
  @Default.String("/tmp/pipelines-export")
  String getLocalExportPath();

  void setLocalExportPath(String localExportPath);
}
