package org.gbif.pipelines.hbase.beam;

import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;
import org.apache.avro.file.CodecFactory;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Executes a pipeline that reads an HBase snapshot and exports verbatim data into a single Avro file
 * using the {@link ExtendedRecord}. schema.
 */
public class ExportHBaseSnapshotSingle {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) {
    PipelineOptionsFactory.register(ExportHBaseOptions.class);
    ExportHBaseOptions options = PipelineOptionsFactory.fromArgs(args).as(ExportHBaseOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);

    Counter recordsExported = Metrics.counter(ExportHBaseSnapshotSingle.class, "recordsExported");
    Counter recordsFailed = Metrics.counter(ExportHBaseSnapshotSingle.class, "recordsFailed");

    //Params
    String exportPath = options.getExportPath();
    Configuration hbaseConfig = hbaseSnapshotConfig(options);

    PCollection<KV<ImmutableBytesWritable, Result>> rows =
        p.apply(
            "Read HBase",
            HadoopFormatIO.<ImmutableBytesWritable, Result>read().withConfiguration(hbaseConfig));

    PCollection<ExtendedRecord> records =
        rows.apply(
            "Convert to extended record",
            ParDo.of(
                new DoFn<KV<ImmutableBytesWritable, Result>, ExtendedRecord>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Result row = c.element().getValue();
                    try {
                      VerbatimOccurrence verbatimOccurrence =
                          OccurrenceConverter.toVerbatimOccurrence(row);
                      c.output(OccurrenceConverter.toExtendedRecord(verbatimOccurrence));
                      recordsExported.inc();
                    } catch (NullPointerException e) {
                      // Expected for bad data
                      recordsFailed.inc();
                    }
                  }
                }));

    records.apply("Write single avro file",
        FileIO.<ExtendedRecord>write()
        .via(AvroIO.sink(ExtendedRecord.class).withCodec(BASE_CODEC))
        .to(exportPath));

    p.run().waitUntilFinish();
  }

  private static Configuration hbaseSnapshotConfig(ExportHBaseOptions options) {
    try {
      Configuration hbaseConf = HBaseConfiguration.create();
      hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, options.getHbaseZk());
      hbaseConf.set("hbase.rootdir", "/hbase");
      hbaseConf.setClass("mapreduce.job.inputformat.class", TableSnapshotInputFormat.class, InputFormat.class);
      hbaseConf.setClass("key.class", ImmutableBytesWritable.class, Writable.class);
      hbaseConf.setClass("value.class", Result.class, Object.class);

      Scan scan = new Scan();
      scan.setBatch(options.getBatchSize()); // for safety
      scan.addFamily("o".getBytes());
      ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
      hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray()));

      // Make use of existing utility methods
      Job job = Job.getInstance(hbaseConf); // creates internal clone of hbaseConf
      TableSnapshotInputFormat.setInput(job, options.getTable(), new Path(options.getRestoreDir()));
      return job.getConfiguration(); // extract the modified clone
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}
