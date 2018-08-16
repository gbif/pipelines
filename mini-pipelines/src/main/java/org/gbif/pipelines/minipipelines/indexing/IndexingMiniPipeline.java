package org.gbif.pipelines.minipipelines.indexing;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.gbif.pipelines.config.EsProcessingPipelineOptions;
import org.gbif.pipelines.indexing.IndexingPipelineRunner;

public class IndexingMiniPipeline {

  /* How to run, as a regular java application (java -cp ...)
   *  Example: java -XX:+UseG1GC -Xms256M -Xmx8G -cp mini-pipelines.jar org.gbif.pipelines.minipipelines.indexing.IndexingMiniPipeline
   *  --targetPath=outputMultimedia --datasetId=cb9beff3-a185-486f-975a-732251444158 --attempt=1 --ESAlias=occurrence
   *  --ESHosts=http://localhost:9200 --runner=SparkRunner
   * */
  public static void main(String[] args) {
    // Create PipelineOptions
    PipelineOptionsFactory.register(EsProcessingPipelineOptions.class);
    IndexingMiniPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(IndexingMiniPipelineOptions.class);

    IndexingPipelineRunner.from(options).run();
  }
}
