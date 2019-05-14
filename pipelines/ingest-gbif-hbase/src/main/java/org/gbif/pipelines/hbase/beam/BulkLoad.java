package org.gbif.pipelines.hbase.beam;

import org.apache.avro.file.CodecFactory;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.hbase.HBaseIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.*;
import java.util.stream.Collectors;

/** Executes a pipeline that reads HBase and exports verbatim data into Avro using the {@link ExtendedRecord}. schema */
public class BulkLoad {

  private static final CodecFactory BASE_CODEC = CodecFactory.snappyCodec();

  public static void main(String[] args) {
    PipelineOptionsFactory.register(BulkLoadOptions.class);
    BulkLoadOptions options = PipelineOptionsFactory.fromArgs(args).as(BulkLoadOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);

    Counter recordsExported = Metrics.counter(BulkLoad.class,"recordsExported");
    Counter recordsFailed = Metrics.counter(BulkLoad.class,"recordsFailed");

    //Params
    String exportPath = options.getExportPath();
    String table =  options.getTable();

    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("hbase.zookeeper.quorum", options.getHbaseZk());

    Scan scan = new Scan();
    scan.setBatch(options.getBatchSize()); // for safety
    scan.addFamily("o".getBytes());

    PCollection<Result> rows =
        p.apply(
            "read",
            HBaseIO.read().withConfiguration(hbaseConfig).withScan(scan).withTableId(table));

    PCollection<ExtendedRecord> records =
        rows.apply(
            "convert",
            ParDo.of(
                new DoFn<Result, ExtendedRecord>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Result row = c.element();
                    try {
                      VerbatimOccurrence verbatimOccurrence = OccurrenceBuilder.buildVerbatimOccurrence(row);
                      ExtendedRecord.Builder builder = ExtendedRecord.newBuilder()
                                                          .setCoreTerms(toVerbatimMap(verbatimOccurrence.getVerbatimFields()));
                      if (Objects.nonNull(verbatimOccurrence.getExtensions())) {
                        builder.setExtensions(toVerbatimExtensionsMap(verbatimOccurrence.getExtensions()));
                      }

                      c.output(builder.build());
                      recordsExported.inc();
                    } catch (NullPointerException e) {
                      // Expected for bad data
                      recordsFailed.inc();
                    }
                  }
                }));

    records.apply("To Avro", AvroIO.write(ExtendedRecord.class).to(exportPath).withSuffix(PipelinesVariables.Pipeline.AVRO_EXTENSION).withCodec(BASE_CODEC));

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }

  /**
   * Transforms a Map<Term,String> into Map<Term.simpleName/String,String>.
   */
  private static Map<String, String> toVerbatimMap(Map<Term,String> verbatimMap) {
    return verbatimMap.entrySet().stream()
            .collect(HashMap::new, (m, v) -> m.put(v.getKey().simpleName(), v.getValue()), HashMap::putAll);
  }

  /**
   * Transforms a Map<Extension, List<Map<Term, String>>> verbatimExtensions into Map<Extension.name()/String, List<Map<Term.simpleName/String, String>>> verbatimExtensions.
   */
  private static Map<String, List<Map<String, String>>> toVerbatimExtensionsMap(Map<Extension, List<Map<Term, String>>> verbatimExtensions) {
    return
            verbatimExtensions.entrySet().stream()
              .collect(HashMap::new,
                      (m, v) -> m.put(v.getKey().name(), v.getValue().stream().map(BulkLoad::toVerbatimMap).collect(Collectors.toList())),
                      HashMap::putAll);
  }
}
