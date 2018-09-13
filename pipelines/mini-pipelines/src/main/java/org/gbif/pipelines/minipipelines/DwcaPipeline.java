package org.gbif.pipelines.minipipelines;

import org.gbif.pipelines.base.options.PipelinesOptionsFactory;
import org.gbif.pipelines.base.pipelines.DwcaToAvroPipeline;
import org.gbif.pipelines.base.pipelines.IndexingWithCreationPipeline;

import org.apache.beam.runners.spark.SparkRunner;

/**
 * Entry point for running one of the four pipelines:
 *
 * <pre>
 *  1) From DwCA to ExtendedRecord *.avro file
 *  2) From DwCA to GBIF interpreted *.avro files
 *  3) From DwCA to Elasticsearch index
 *  4) From GBIF interpreted *.avro files to Elasticsearch index
 * </pre>
 *
 * How to run:
 *
 * <pre>{@code
 * java -jar target/mini-pipelines-0.1-SNAPSHOT-shaded.jar examples/configs/mini.dwca2avro.properties(or mini.indexing.properties/mini.interpretation.properties/mini.interpretation2es.properties)
 * }</pre>
 */
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
