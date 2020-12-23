package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Pipeline options necessary for JackKnife */
public interface JackKnifePipelineOptions
    extends PipelineOptions, InterpretationPipelineOptions, AllDatasetsPipelinesOptions {

  @Description("Minimum number of values required for a JackKnife model.")
  @Default.Integer(80)
  Integer getMinSampleThreshold();

  void setMinSampleThreshold(Integer minSampleThreshold);

  @Description("Comma delimited list of layer features to use for JackKnife Outliers.")
  @Default.String("")
  String getLayers();

  void setLayers(String layers);

  @Description("Path to JackKnife avro files")
  @Default.String("")
  String getPath();

  void setPath(String path);
}
