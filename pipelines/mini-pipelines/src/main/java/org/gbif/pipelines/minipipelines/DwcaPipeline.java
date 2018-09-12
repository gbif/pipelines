package org.gbif.pipelines.minipipelines;

import org.gbif.pipelines.base.options.PipelinesOptionsFactory;
import org.gbif.pipelines.base.pipelines.DwcaToAvroPipeline;
import org.gbif.pipelines.base.pipelines.IndexingWithCreationPipeline;

import org.apache.beam.runners.spark.SparkRunner;

public class DwcaPipeline {

  public static void main(String[] args) {

    // Create PipelineOptions
    DwcaPipelineOptions options = PipelinesOptionsFactory.create(DwcaPipelineOptions.class, args);
    options.setRunner(SparkRunner.class);

    switch (options.getPipelineStep()) {
      case DWCA_TO_AVRO:
        DwcaToAvroPipeline.createAndRun(options);
        break;
      case INTERPRET:
        DwcaInterpretationPipeline.createAndRun(options);
        break;
      case INDEX_TO_ES:
        DwcaIndexingPipeline.createAndRun(options);
        break;
      case INTERPRET_TO_INDEX:
        IndexingWithCreationPipeline.createAndRun(options);
        break;
      default:
        break;
    }
  }
}
