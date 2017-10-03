package org.gbif.data.pipelines;

import org.gbif.data.io.avro.ExtendedRecord;
import org.gbif.data.io.avro.UntypedOccurrence;
import org.gbif.data.pipelines.io.dwca.hdfs.DwCAInputFormat;

import java.util.Collections;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemRegistrar;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;

/**
 * First demonstration only!

 */
public class TestSOLR {

  public static void main(String[] args) {

    Configuration hadoopConf = new Configuration(); // assume defaults on CP
    HadoopFileSystemOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(hadoopConf));
    options.setRunner(SparkRunner.class); // forced

    Pipeline p = Pipeline.create(options);

    p.getCoderRegistry().registerCoderForClass(UntypedOccurrence.class, AvroCoder.of(UntypedOccurrence.class));
    p.getCoderRegistry().registerCoderForClass(ExtendedRecord.class,  AvroCoder.of(ExtendedRecord.class));

    hadoopConf.setClass("mapreduce.job.inputformat.class", DwCAInputFormat.class, InputFormat.class);
    hadoopConf.setStrings("mapreduce.input.fileinputformat.inputdir", "hdfs://nameservice1/tmp/dwca.zip");
    hadoopConf.setClass("key.class", Text.class, Object.class);
    hadoopConf.setClass("value.class", ExtendedRecord.class, Object.class);

    HadoopFileSystemRegistrar hdfs = new HadoopFileSystemRegistrar();


    PCollection<KV<Text,ExtendedRecord>> rawRecords =
      p.apply("read", HadoopInputFormatIO.<Text, ExtendedRecord>read().withConfiguration(hadoopConf));

    // Convert the record type into an UntypedOccurrence record
    PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
      "ParseRawToDwC", ParDo.of(new ParseDwC2()));

    PCollectionList<UntypedOccurrence> partitioned = verbatimRecords.apply(Partition.of(10, new RecordPartitioner()));

    // Write the file to SOLR
    final SolrIO.ConnectionConfiguration conn = SolrIO.ConnectionConfiguration
      .create("c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181/solr5dev");

    PCollection<SolrInputDocument> inputDocs = partitioned.apply(Flatten.pCollections()).apply(
      "Convert to SOLR", ParDo.of(new ParseToSOLRDoc()));

    inputDocs.apply(SolrIO.write().to("tim-untyped").withConnectionConfiguration(conn));


    PipelineResult result = p.run();
    // Note: can read result state here (e.g. a web app polling for metrics would do this)
    result.waitUntilFinish();
  }




  /**
   * Hashcode partitioner (for demo only)
   */
  public static class RecordPartitioner implements Partition.PartitionFn<UntypedOccurrence> {
    @Override
    public int partitionFor(UntypedOccurrence elem, int numPartitions) {
      int hc = elem.hashCode();
      int hc2 = hc<0 ? -hc : hc;
      return hc2 % numPartitions;
    }
  }
}
