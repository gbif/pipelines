package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Description;

/** Main pipeline options necessary for DwCA export for Living atlases */
public interface DwCAExportPipelineOptions extends IndexingPipelineOptions {

  @Description("Message format compatible Image service URL path")
  String getImageServicePath();

  void setImageServicePath(String imageServicePath);
}
