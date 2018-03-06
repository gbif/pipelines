package org.gbif.pipelines.indexing;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Throwables;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline to build ElasticSearch from Avro files containing ExtendedRecord data.
 * <p>
 * The core fields are extracted and the simple name is used for the ES fields (no namespace).  A location
 * field is added suitable for `geopoint` indexing.
 * <p>
 * NOTE: This is a transient piece of code and will be removed when the interpretation pipelines are available.
 * As such code contains hard coded values and is not suitable for production use.
 */
public class Avro2ElasticSearchPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(Avro2ElasticSearchPipeline.class);

  private static final String SOURCE_PATH = "hdfs://ha-nn/pipelines/data/aves_large.avro/*";
  private static final String[] ES_HOSTS =
    new String[] {"http://c4n1.gbif.org:9200", "http://c4n2.gbif.org:9200", "http://c4n3.gbif.org:9200",
      "http://c4n4.gbif.org:9200", "http://c4n5.gbif.org:9200", "http://c4n6.gbif.org:9200",
      "http://c4n7.gbif.org:9200", "http://c4n8.gbif.org:9200", "http://c4n9.gbif.org:9200"};

  private static final String ES_INDEX = "tim";
  private static final String ES_TYPE = "tim";
  private static final int BATCH_SIZE = 1000;

  public static void main(String[] args) {

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    Pipeline p = Pipeline.create(options);

    // Read Avro files
    PCollection<ExtendedRecord> verbatimRecords =
      p.apply("Read Avro files", AvroIO.read(ExtendedRecord.class).from(SOURCE_PATH));

    // Convert to JSON
    PCollection<String> json = verbatimRecords.apply("Convert to JSON", ParDo.of(new RecordFormatter()));

    // Write the file to ES
    ElasticsearchIO.ConnectionConfiguration conn =
      ElasticsearchIO.ConnectionConfiguration.create(ES_HOSTS, ES_INDEX, ES_TYPE);

    // Index in ES
    json.apply(ElasticsearchIO.write().withConnectionConfiguration(conn).withMaxBatchSize(BATCH_SIZE));

    // instruct the writer to use a provided document ID
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

  /**
   * Returns a flat JSON String holding the core records only stripped of namespaces and with the addition of a
   * location field suitable for geopoint indexing in ElasticSearch.
   */
  public static class RecordFormatter extends DoFn<ExtendedRecord, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      ExtendedRecord record = c.element();
      Map<String, String> terms = record.getCoreTerms();
      Map<String, String> stripped = new HashMap<>(record.getCoreTerms().size());

      terms.forEach((k, v) -> stripped.put(stripNS(k), v));

      // location suitable for geopoint format
      if (stripped.get("decimalLatitude") != null && stripped.get("decimalLongitude") != null) {
        stripped.put("location", stripped.get("decimalLatitude") + "," + stripped.get("decimalLongitude"));
      }

      try {
        c.output(new ObjectMapper().writeValueAsString(stripped));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  static String stripNS(String source) {
    return source.substring(source.lastIndexOf('/') + 1);
  }
}
