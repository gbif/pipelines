package org.gbif.pipelines.standalone;

import org.gbif.pipelines.ingest.options.DwcaPipelineOptions;
import org.gbif.pipelines.ingest.options.PipelinesOptionsFactory;
import org.gbif.pipelines.ingest.pipelines.DwcaToAvroPipeline;
import org.gbif.pipelines.ingest.pipelines.DwcaToEsIndexPipeline;
import org.gbif.pipelines.ingest.pipelines.DwcaToIngestPipeline;
import org.gbif.pipelines.ingest.pipelines.IngestToEsIndexExtendedPipeline;

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
 * java -jar target/ingest-gbif-standalone-0.1-SNAPSHOT-shaded.jar  --datasetId=0057a720-17c9-4658-971e-9578f3577cf5
 *  --attempt=1
 *  --pipelineStep=DWCA_TO_AVRO
 *  --targetPath=/some/path/to/output/
 *  --inputPath=/some/path/to/input/dwca/dwca.zip
 *
 * or
 *
 * java -jar target/ingest-gbif-standalone-0.1-SNAPSHOT-shaded.jar dwca2avro.properties
 *
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
        DwcaToIngestPipeline.createAndRun(options);
        break;
      case INDEX_TO_ES:
        DwcaToEsIndexPipeline.createAndRun(options);
        break;
      case INTERPRET_TO_INDEX:
        IngestToEsIndexExtendedPipeline.createAndRun(options);
        break;
      default:
        break;
    }
  }
}
