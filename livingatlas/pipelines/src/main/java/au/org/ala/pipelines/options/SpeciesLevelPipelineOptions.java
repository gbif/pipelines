package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

public interface SpeciesLevelPipelineOptions extends InterpretationPipelineOptions {

  @Description("Default directory where species level information is stored to")
  String getSpeciesAggregatesPath();

  void setSpeciesAggregatesPath(String speciesAggregatesPath);
}
