package au.org.ala.pipelines.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;

/** Main pipeline options necessary for SOLR index for Living atlases */
public interface IndexingPipelineOptions
    extends PipelineOptions,
        InterpretationPipelineOptions,
        SpeciesLevelPipelineOptions,
        AllDatasetsPipelinesOptions {

  @Description("Include references to image service objects")
  @Default.Boolean(false)
  Boolean getIncludeImages();

  void setIncludeImages(Boolean includeImages);

  @Description("Include references to species list objects")
  @Default.Boolean(false)
  Boolean getIncludeSpeciesLists();

  void setIncludeSpeciesLists(Boolean includeSpeciesLists);

  @Description("Include gbif taxonomy")
  @Default.Boolean(false)
  Boolean getIncludeGbifTaxonomy();

  void setIncludeGbifTaxonomy(Boolean includeGbifTaxonomy);

  @Description("Include sensitive data checks")
  @Default.Boolean(true)
  Boolean getIncludeSensitiveDataChecks();

  void setIncludeSensitiveDataChecks(Boolean includeSensitiveDataChecks);
}
