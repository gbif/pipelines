package org.gbif.pipelines.minipipelines.indexing;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.gbif.pipelines.config.EsProcessingPipelineOptions;

public interface IndexingMiniPipelineOptions
    extends EsProcessingPipelineOptions, SparkPipelineOptions {}
