package org.gbif.pipelines.indexing;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.TypeDescriptors;
import org.gbif.pipelines.core.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.core.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.hadoop.io.DwCAInputFormat;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline that reads an DwC-A file and writes it as an avro file containing Extended Records.
 *
 * TODO: A lot of hard coded stuff here to sort out...
 */
public class DwCA2AvroPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DwCA2AvroPipeline.class);

  public static void main(String[] args) {
    Configuration conf = new Configuration(); // assume defaults on CP
    conf.setClass("mapreduce.job.inputformat.class", DwCAInputFormat.class, InputFormat.class);
    conf.setStrings("mapreduce.input.fileinputformat.inputdir", "hdfs://ha-nn/tmp/dwca-lep5.zip");
    conf.setClass("key.class", Text.class, Object.class);
    conf.setClass("value.class", ExtendedRecord.class, Object.class);

    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(conf, args);
    Pipeline p = Pipeline.create(options);
    Coders.registerAvroCoders(p, UntypedOccurrence.class, TypedOccurrence.class, ExtendedRecord.class);

    PCollection<KV<Text, ExtendedRecord>> rawRecords =
      p.apply("Read DwC-A", HadoopInputFormatIO.<Text, ExtendedRecord>read().withConfiguration(conf));

    PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
      "Convert to Avro", MapElements.into(TypeDescriptors.untypedOccurrence())
        .via(x -> FunctionFactory.untypedOccurrenceBuilder().apply(x.getValue())));

    verbatimRecords.apply(
      "Write Avro files", AvroIO.write(UntypedOccurrence.class).to("hdfs://ha-nn/tmp/dwca-lep5.avro"));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
