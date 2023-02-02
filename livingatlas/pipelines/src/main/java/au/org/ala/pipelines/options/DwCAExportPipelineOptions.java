package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Main pipeline options necessary for DwCA export for Living atlases */
public interface DwCAExportPipelineOptions extends IndexingPipelineOptions {

  @Description("Message format compatible Image service URL path")
  String getImageServicePath();

  void setImageServicePath(String imageServicePath);

  @Description("Local filesystem path to use to generate a path for ZIP")
  String getLocalExportPath();

  void setLocalExportPath(String localExportPath);

  @Default.Boolean(true)
  @Description("Predicate exports enabled")
  Boolean getPredicateExportEnabled();

  void setPredicateExportEnabled(Boolean predicateExportEnabled);
}
