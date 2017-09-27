package org.gbif.data.pipelines;

import org.gbif.data.io.avro.ExtendedRecord;
import org.gbif.data.io.avro.UntypedOccurrence;
import org.gbif.data.pipelines.io.dwca.DwCIO;
import org.gbif.dwca.record.StarRecord;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;

/**
 * First demonstration only!
 */
public class Test {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class); // first testing only
    Pipeline p = Pipeline.create(options);

    PCollection<ExtendedRecord> rawRecords = p.apply(
      "ReadRawRecords", DwCIO.Read.withPaths(Paths.get("/tmp/dwca.zip"), Paths.get("/tmp/working")));

    // Repartition data for a test
    rawRecords.apply(Partition.of(10, new Partition.PartitionFn<ExtendedRecord>() {
      public int partitionFor(ExtendedRecord raw, int numPartitions) {
        int hc = raw.hashCode();
        if (hc<0) hc*=-1;
        return hc % numPartitions;
      }}));

    PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
      "ParseRawToDwC", ParDo.of(new ParseDwC()));

    // Write the file to Avro
    verbatimRecords.apply(AvroIO.write(UntypedOccurrence.class).to("output/transformed"));

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }
}
