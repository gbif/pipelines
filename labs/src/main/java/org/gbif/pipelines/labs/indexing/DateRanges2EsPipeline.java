package org.gbif.pipelines.labs.indexing;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FOR DEMO ONLY, CONTAINS LOTS OF HARD CODED VALUES
 */
public class DateRanges2EsPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DateRanges2EsPipeline.class);

  private static final String ES_IDX = "es-date-ranges";
  private static final String[] ES_ADDRESSES = {"http://c3n1.gbif.org:9200", "http://c3n2.gbif.org:9200", "http://c3n3.gbif.org:9200"};
  private static final String FILE_PATH = "labs/data/data-ranges/es/es-date-ranges.txt";
  private static final String DELETE_IDX = String.format("curl -X DELETE %s/%s", ES_ADDRESSES[0], ES_IDX);
  private static final String CREATE_IDX = String.format("curl -X PUT %s/%s -d @labs/data/data-ranges/es/es-index-mapping.json", ES_ADDRESSES[0], ES_IDX);

  public static void main(String[] args) throws Exception {

    // Prestep: Recreate ES index
    Runtime.getRuntime().exec(DELETE_IDX).waitFor();
    Runtime.getRuntime().exec(CREATE_IDX).waitFor();

    // Step 0: Options
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);

    Pipeline p = Pipeline.create(options);

    // Step 1: Read file
    PCollection<String> rawCollection = p.apply(TextIO.read().from(FILE_PATH));

    // Step 2: Write ES index
    final ElasticsearchIO.ConnectionConfiguration conn = ElasticsearchIO.ConnectionConfiguration.create(ES_ADDRESSES, ES_IDX, ES_IDX);
    rawCollection.apply(ElasticsearchIO.write().withConnectionConfiguration(conn));

    // Step 3: Run pipeline
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());

  }

}
