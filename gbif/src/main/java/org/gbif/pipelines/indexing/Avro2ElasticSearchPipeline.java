package org.gbif.pipelines.indexing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.descriptor.CustomTypeDescriptors;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrenceLowerCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline that reads an Avro file and indexes it into Elastic Search.
 *
 * TODO: A lot of hard coded stuff here to sort out...
 */
public class Avro2ElasticSearchPipeline extends AbstractSparkOnYarnPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Avro2ElasticSearchPipeline.class);

  /*
  private static final String SOURCE_PATH = "hdfs://ha-nn/user/hive/warehouse/tim.db/occ_all/*";
  private static final String[] ES_HOSTS =
    new String[] {"http://c4n1.gbif.org:9200",
      "http://c4n2.gbif.org:9200",
      "http://c4n3.gbif.org:9200",
      "http://c4n4.gbif.org:9200",
      "http://c4n5.gbif.org:9200",
      "http://c4n6.gbif.org:9200",
      "http://c4n7.gbif.org:9200",
      "http://c4n8.gbif.org:9200",
      "http://c4n9.gbif.org:9200"};
  */
  private static final String SOURCE_PATH = "hdfs://ha-nn/user/hive/warehouse/occ_all/*";
  private static final String[] ES_HOSTS =
    new String[] {"http://c3n1.gbif.org:9200",
      "http://c3n2.gbif.org:9200",
      "http://c3n3.gbif.org:9200"};

  private static final String ES_INDEX = "occurrence";
  private static final String ES_TYPE = "occurrence";
  private static final int BATCH_SIZE = 1000;

  public static void main(String[] args) {

    Configuration conf = new Configuration(); // assume defaults on CP
    Pipeline p = newPipeline(args, conf);
    Coders.registerAvroCoders(p, UntypedOccurrenceLowerCase.class, TypedOccurrence.class);

    // Read Avro files
    PCollection<UntypedOccurrenceLowerCase> verbatimRecords = p.apply(
      "Read Avro files", AvroIO.read(UntypedOccurrenceLowerCase.class).from(SOURCE_PATH));

    // Convert the objects (interpretation)
    PCollection<TypedOccurrence> interpreted = verbatimRecords.apply("Interpret occurrence records"
      , MapElements.into(CustomTypeDescriptors.typedOccurrencies()).via(FunctionFactory.interpretOccurrenceLowerCase()::apply));

    // Do the nub lookup
/*    PCollection<TypedOccurrence> matched = interpreted.apply("Align to backbone using species/match"
      , MapElements.into(CustomTypeDescriptors.typedOccurrencies()).via(FunctionFactory.gbifSpeciesMatch()::apply));*/

    // Convert to JSON
    PCollection<String> json = interpreted.apply("Convert to JSON"
      , MapElements.into(TypeDescriptors.strings()).via(TypedOccurrence::toString));

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
