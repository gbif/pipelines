package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

public interface SpeciesLevelPipelineOptions extends InterpretationPipelineOptions {

  @Description("Default directory where species level information is stored to")
  String getSpeciesAggregatesPath();

  void setSpeciesAggregatesPath(String speciesAggregatesPath);

  @Description("The max age in minutes of the species list download. Default is 1440 = 1 day")
  @Default.Long(1440l)
  long getMaxDownloadAgeInMinutes();

  void setMaxDownloadAgeInMinutes(long getMaxDownloadAgeInMinutes);
}
