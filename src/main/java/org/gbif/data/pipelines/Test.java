package org.gbif.data.pipelines;

import org.gbif.data.io.avro.UntypedOccurrence;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

/**
 * First demonstration only!
 */
public class Test {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class); // first testing only
    Pipeline p = Pipeline.create(options);

    PCollection<String> rawRecords = p.apply(
      "ReadRawRecords", TextIO.read().from("/tmp/occurrence.txt"));

    // Repartition data for a
    rawRecords.apply(Partition.of(10, new Partition.PartitionFn<String>() {
      public int partitionFor(String raw, int numPartitions) {
        int hc = raw.hashCode();
        if (hc<0) hc*=-1;
        return hc % numPartitions;
      }}));

    // Parse the raw record into a DwC file
    PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
      "ParseRawToDwC", ParDo.of(new ParseDwC()));


    // Write the file to Avro
    verbatimRecords.apply(AvroIO.write(UntypedOccurrence.class).to("transformed"));

    PipelineResult result = p.run();
    result.waitUntilFinish();
  }
}
