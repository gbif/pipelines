package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Options for pipelines that use species level information. */
public interface SpeciesLevelPipelineOptions extends InterpretationPipelineOptions {

  @Description("Default directory where species level information is stored to")
  String getSpeciesAggregatesPath();

  void setSpeciesAggregatesPath(String speciesAggregatesPath);

  @Description("Default directory where species list level information is stored to")
  @Default.String("/species-lists/species-lists.avro")
  String getSpeciesListCachePath();

  void setSpeciesListCachePath(String speciesListCachePath);

  @Description("The max age in minutes of the species list download. Default is 1440 = 1 day")
  @Default.Long(1440L)
  long getMaxDownloadAgeInMinutes();

  void setMaxDownloadAgeInMinutes(long getMaxDownloadAgeInMinutes);

  @Default.Boolean(true)
  Boolean getIncludeConservationStatus();

  void setIncludeConservationStatus(Boolean includeConservationStatus);

  @Default.Boolean(true)
  Boolean getIncludeInvasiveStatus();

  void setIncludeInvasiveStatus(Boolean includeInvasiveStatus);

  @Default.Boolean(false)
  Boolean getUseSpeciesListV2();

  void setUseSpeciesListV2(Boolean setUseSpeciesListV2);
}
