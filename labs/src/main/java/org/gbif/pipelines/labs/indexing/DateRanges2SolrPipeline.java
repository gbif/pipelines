package org.gbif.pipelines.labs.indexing;

import java.util.regex.Pattern;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FOR DEMO ONLY, CONTAINS LOTS OF HARD CODED VALUES
 *
 * Use solr-setup.md as an example how to upload a solr schema (labs/data/data-ranges/solr/conf)
 */
public class DateRanges2SolrPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(DateRanges2SolrPipeline.class);

  private static final String FIELD_ID = "id";
  private static final String FIELD_NAME = "eventDate";
  private static final Pattern RGX = Pattern.compile(",");

  private static final String SOLR_IDX = "solr-date-range";
  private static final String FILE_PATH = "labs/data/data-ranges/solr/solr-date-ranges.txt";
  private static final String SOLR_HOST = "c3master1-vh.gbif.org:2181,c3master2-vh.gbif.org:2181,c3master3-vh.gbif.org:2181/solr5c2";

  private static final String SERVER_PATH = "http://c3n1.gbif.org:8983/solr/admin/collections?action=";
  private static final String DELETE = "%sDELETE&name=%s";
  private static final String CREATE = "%sCREATE&name=%s&numShards=1&replicationFactor=1&maxShardsPerNode=1&collection.configName=%s";

  private static final String DELETE_IDX = String.format(DELETE, SERVER_PATH, SOLR_IDX);
  private static final String CREATE_IDX = String.format(CREATE, SERVER_PATH, SOLR_IDX, SOLR_IDX);

  private static final float DEFAULT_BOOST = 1f;

  public static void main(String[] args) throws Exception {

    // Prestep: Recreate SOLR index
    Runtime.getRuntime().exec(DELETE_IDX).waitFor();
    Runtime.getRuntime().exec(CREATE_IDX).waitFor();

    // Step 0: Options
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);

    Pipeline p = Pipeline.create(options);

    // Step 1: Read file
    PCollection<String> rawCollection = p.apply(TextIO.read().from(FILE_PATH));

    // Step 2: Convert string to SOLR document
    PCollection<SolrInputDocument> inputDocs = rawCollection.apply(ParDo.of(new DoFn<String, SolrInputDocument>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        String element = c.element();

        String[] split = RGX.split(element);

        SolrInputDocument outputRecord = new SolrInputDocument();

        SolrInputField idField = new SolrInputField(FIELD_ID);
        idField.addValue(split[0], DEFAULT_BOOST);

        SolrInputField inputField = new SolrInputField(FIELD_NAME);
        inputField.addValue(split[1], DEFAULT_BOOST);

        outputRecord.put(FIELD_ID, idField);
        outputRecord.put(FIELD_NAME, inputField);

        c.output(outputRecord);
      }
    }));

    // Step 3: Write SOLR index
    final SolrIO.ConnectionConfiguration conn = SolrIO.ConnectionConfiguration.create(SOLR_HOST);
    inputDocs.apply(SolrIO.write().to(SOLR_IDX).withConnectionConfiguration(conn));

    // Step 4: Run pipeline
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
