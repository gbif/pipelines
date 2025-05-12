package org.gbif.pipelines.common.beam.options;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.gbif.api.model.pipelines.StepType;
import org.gbif.api.vocabulary.DatasetType;

/**
 * Pipeline options (configuration) for GBIF based data indexing pipelines. Optionally can use a
 * {@link HadoopFileSystemOptions} when exporting/reading files.
 */
public interface EsIndexingPipelineOptions
    extends EsPipelineOptions, InterpretationPipelineOptions {

  @Description("StepType to run for this indexing stage")
  @Default.Enum("INTERPRETED_TO_INDEX")
  StepType getStepType();

  void setStepType(StepType stepType);

  @Description("DatasetType to run for this indexing stage")
  @Default.Enum("OCCURRENCE")
  DatasetType getDatasetType();

  void setDatasetType(DatasetType datasetType);

  @Description("Whether to index legacy taxonomy")
  @Default.Boolean(true)
  Boolean isIndexLegacyTaxonomy();

  void setIndexLegacyTaxonomy(Boolean indexLegacyTaxonomy);

  @Description("Whether to index legacy taxonomy")
  @Default.Boolean(true)
  Boolean isIndexMultiTaxonomy();

  void setIndexMultiTaxonomy(Boolean indexMultiTaxonomy);
}
