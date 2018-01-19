package org.gbif.pipelines.demo;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.descriptor.CustomTypeDescriptors;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.demo.transformation.validator.UniqueOccurrenceIdTransformation;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrenceLowerCase;

import java.util.Collections;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UniquenessValidationPipelineDemo {

  private static final Logger LOG = LoggerFactory.getLogger(UniquenessValidationPipelineDemo.class);
  private static final String SOURCE_PATH = "hdfs://ha-nn/user/hive/warehouse/occ_all/*";
  private static final String[] ES_HOSTS =
    {"http://c3n1.gbif.org:9200", "http://c3n2.gbif.org:9200", "http://c3n3.gbif.org:9200"};

  private static final String ES_INDEX = "uniqueness";
  private static final int BATCH_SIZE = 1000;

  public static void main(String... args) {

    //Setting up options and creating pipeline
    Configuration conf = new Configuration(); // assume defaults on CP
    HadoopFileSystemOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(conf));
    Pipeline p = Pipeline.create(options);

    //Registering avro coders
    Coders.registerAvroCoders(p, UntypedOccurrenceLowerCase.class, TypedOccurrence.class);

    // Read Avro files
    PCollection<UntypedOccurrenceLowerCase> read =
      p.apply("Read Avro files", AvroIO.read(UntypedOccurrenceLowerCase.class).from(SOURCE_PATH));

    UniqueOccurrenceIdTransformation transformation = new UniqueOccurrenceIdTransformation();

    //Filter uniqueness occurrenceId
    PCollectionTuple filtered = read.apply(transformation);

    // Convert the objects (interpretation)
    PCollection<TypedOccurrence> converted = filtered.get(transformation.getDataTag())
      .apply("Interpret occurrence records",
             MapElements.into(CustomTypeDescriptors.typedOccurrencies())
               .via(FunctionFactory.interpretOccurrenceLowerCase()::apply));

    // Convert to JSON
    PCollection<String> json =
      converted.apply("Convert to JSON", MapElements.into(TypeDescriptors.strings()).via(TypedOccurrence::toString));

    // Write the file to ES
    ElasticsearchIO.ConnectionConfiguration conn =
      ElasticsearchIO.ConnectionConfiguration.create(ES_HOSTS, ES_INDEX, ES_INDEX);

    // Index in ES
    json.apply(ElasticsearchIO.write().withConnectionConfiguration(conn).withMaxBatchSize(BATCH_SIZE));

    // instruct the writer to use a provided document ID
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
