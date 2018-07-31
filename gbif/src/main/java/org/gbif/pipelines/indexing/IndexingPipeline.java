package org.gbif.pipelines.indexing;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.EsProcessingPipelineOptions;

public class IndexingPipeline {

  /* How to run, as a regular java application (java -cp ...)
   *  --wsProperties=/Users/cgp440/Projects/GBIF/pipelines-example/src/main/resources/ws.properties
   *  --datasetId=0021cd20-de15-4eba-837f-335c00c154dd --attempt=1 --runner=DirectRunner
   *  --defaultTargetDirectory=/Users/cgp440/Projects/GBIF/tmp/dwca-finished/
   *  --inputFile=/Users/cgp440/Projects/GBIF/tmp/dwca-finished/ --avroCompressionType=DEFLATE
   *  --hdfsTempLocation=/Users/cgp440/Projects/GBIF/tmp/temp/
   *
   * */
  public static void main(String[] args) {
    // Create PipelineOptions
    EsProcessingPipelineOptions options = DataPipelineOptionsFactory.createForEs(args);

    IndexingPipelineRunner.from(options).run();
  }
}
