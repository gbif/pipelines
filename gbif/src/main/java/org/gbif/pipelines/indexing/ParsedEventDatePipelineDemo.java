package org.gbif.pipelines.indexing;

import org.gbif.pipelines.common.beam.BeamFunctions;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.InterpretOccurrenceEventDate;
import org.gbif.pipelines.io.avro.ParsedEventDateDemo;
import org.gbif.pipelines.io.avro.UntypedOccurrenceLowerCase;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParsedEventDatePipelineDemo extends AbstractSparkOnYarnPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(ParsedEventDatePipelineDemo.class);

  private static final String SOURCE_PATH = "hdfs://ha-nn/user/hive/warehouse/occ_all/*";
  private static final String[] ES_HOSTS =
    new String[] {"http://c3n1.gbif.org:9200",
      "http://c3n2.gbif.org:9200",
      "http://c3n3.gbif.org:9200"};

  private static final String ES_INDEX = "uniqueness";
  private static final String ES_TYPE = "uniqueness";
  private static final int BATCH_SIZE = 1000;

  public static void main(String[] args) {

    Configuration conf = new Configuration(); // assume defaults on CP
    Pipeline p = newPipeline(args, conf);
    Coders.registerAvroCoders(p, UntypedOccurrenceLowerCase.class, ParsedEventDateDemo.class);

    // Read Avro files
    PCollection<UntypedOccurrenceLowerCase> verbatimRecords = p.apply(
      "Read Avro files", AvroIO.read(UntypedOccurrenceLowerCase.class).from(SOURCE_PATH));

    // Convert the objects (interpretation)
    PCollection<ParsedEventDateDemo> interpreted = verbatimRecords.apply(
      "Interpret occurrence records", ParDo.of(BeamFunctions.beamify(new InterpretOccurrenceEventDate())))
                                                              .setCoder(AvroCoder.of(ParsedEventDateDemo.class));

    // Convert to JSON
    PCollection<String> json = interpreted.apply(
      "Convert to JSON", ParDo.of(BeamFunctions.asJson(ParsedEventDateDemo.class)));

    // Write the file to ES
    ElasticsearchIO.ConnectionConfiguration conn = ElasticsearchIO.ConnectionConfiguration
      .create(ES_HOSTS,ES_INDEX, ES_TYPE);

    // Index in ES
    json.apply(ElasticsearchIO.write().withConnectionConfiguration(conn).withMaxBatchSize(BATCH_SIZE));

    // instruct the writer to use a provided document ID
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }
}
