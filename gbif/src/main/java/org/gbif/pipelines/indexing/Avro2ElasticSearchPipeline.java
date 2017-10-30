package org.gbif.pipelines.indexing;

import org.gbif.pipelines.core.beam.BeamFunctions;
import org.gbif.pipelines.core.beam.Coders;
import org.gbif.pipelines.core.functions.Functions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

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

/**
 * A pipeline that reads an Avro file and indexes it into Elastic Search.
 *
 * TODO: A lot of hard coded stuff here to sort out...
 */
public class Avro2ElasticSearchPipeline extends AbstractSparkOnYarnPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Avro2ElasticSearchPipeline.class);

  private static final String SOURCE_PATH = "hdfs://ha-nn/tmp/dwca-lep5.avro-00000-of-00001";
  private static final String[] ES_HOSTS =
    new String[] {"http://c3n1.gbif.org:9200","http://c3n2.gbif.org:9200","http://c3n3.gbif.org:9200"};
  private static final String ES_INDEX = "occurrence";
  private static final String ES_TYPE = "occurrence";
  private static final int BATCH_SIZE = 1000;

  public static void main(String[] args) {
    Configuration conf = new Configuration(); // assume defaults on CP
    Pipeline p = newPipeline(args, conf);
    Coders.registerAvroCoders(p, UntypedOccurrence.class, TypedOccurrence.class, ExtendedRecord.class);

    // Read Avro files
    PCollection<UntypedOccurrence> verbatimRecords = p.apply(
      "Read Avro files", AvroIO.read(UntypedOccurrence.class).from(SOURCE_PATH));

    // Convert the objects (interpretation)
    PCollection<TypedOccurrence> interpreted = verbatimRecords.apply(
      "Interpret occurrence records", ParDo.of(BeamFunctions.beamify(Functions.interpretOccurrence())))
                                                              .setCoder(AvroCoder.of(TypedOccurrence.class));

    // Do the nub lookup
    PCollection<TypedOccurrence> matched = interpreted.apply(
      "Align to backbone using species/match", ParDo.of(
        BeamFunctions.beamify(Functions.gbifSpeciesMatch("https://api.gbif.org/"))))
                                                      .setCoder(AvroCoder.of(TypedOccurrence.class));

    // Convert to JSON
    PCollection<String> json = matched.apply(
      "Convert to JSON", ParDo.of(BeamFunctions.asJson(TypedOccurrence.class)));

    // Write the file to ES
    ElasticsearchIO.ConnectionConfiguration conn = ElasticsearchIO.ConnectionConfiguration
      .create(ES_HOSTS,ES_INDEX, ES_TYPE);

    // Index in ES
    json.apply(ElasticsearchIO.write().withConnectionConfiguration(conn).withMaxBatchSize(BATCH_SIZE));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }
}
