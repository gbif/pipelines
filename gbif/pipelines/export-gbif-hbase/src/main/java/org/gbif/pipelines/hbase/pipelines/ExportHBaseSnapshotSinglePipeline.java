package org.gbif.pipelines.hbase.pipelines;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.pipelines.hbase.options.ExportHBaseOptions;
import org.gbif.pipelines.hbase.utils.ConfigurationFactory;
import org.gbif.pipelines.hbase.utils.OccurrenceConverter;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Executes a pipeline that reads an HBase snapshot and exports verbatim data into a single Avro
 * file using the {@link ExtendedRecord}. schema.
 */
public class ExportHBaseSnapshotSinglePipeline {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) {
    PipelineOptionsFactory.register(ExportHBaseOptions.class);
    ExportHBaseOptions options = PipelineOptionsFactory.fromArgs(args).as(ExportHBaseOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);

    Counter recordsExported =
        Metrics.counter(ExportHBaseSnapshotSinglePipeline.class, "recordsExported");
    Counter recordsFailed =
        Metrics.counter(ExportHBaseSnapshotSinglePipeline.class, "recordsFailed");

    // Params
    String exportPath = options.getExportPath();
    Configuration hbaseConfig = ConfigurationFactory.create(options);

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

    records.apply(
        "Write single avro file",
        FileIO.<ExtendedRecord>write()
            .via(AvroIO.sink(ExtendedRecord.class).withCodec(BASE_CODEC))
            .to(exportPath));

    p.run().waitUntilFinish();
  }
}
