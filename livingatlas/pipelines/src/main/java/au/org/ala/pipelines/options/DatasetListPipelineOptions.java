package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

public interface DatasetListPipelineOptions extends InterpretationPipelineOptions {

  @Description("Directory for DWCA imports")
  String getDwcaImportPath();

  void setDwcaImportPath(String dwcaImportPath);
}
