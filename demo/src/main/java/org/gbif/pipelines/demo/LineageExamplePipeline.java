package org.gbif.pipelines.demo;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A demonstration showing how one MIGHT(!) handle lineage across a pipleine.
 * The demo is intending to show how you can fan out multiple output values using the TupleTag functionality of
 * beam.
 */
public class LineageExamplePipeline {

  private static final Logger LOG = LoggerFactory.getLogger(LineageExamplePipeline.class);

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    //options.setRunner(DirectRunner.class); // forced for this demo
    Pipeline p = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(p, ExtendedRecord.class, UntypedOccurrence.class);

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords =
      p.apply("Read from Darwin Core Archive", DwCAIO.Read.withPaths("demo/dwca.zip", "demo/target/tmp"));

    // Define the output collections for the transformed data and for the lineage (here just as strings)
    final TupleTag<UntypedOccurrence> data = new TupleTag<UntypedOccurrence>() {};
    final TupleTag<KV<String, String>> lineage = new TupleTag<KV<String, String>>() {}; // one way of modeling it

    PCollectionTuple results = rawRecords.apply(ParDo.of(new DoFn<ExtendedRecord, UntypedOccurrence>() {

      @ProcessElement
      public void processElement(ProcessContext c) {
        ExtendedRecord input = c.element();
        UntypedOccurrence output = FunctionFactory.untypedOccurrenceBuilder().apply(input);
        // make up some bogus statements about lineage
        String key = output.getOccurrenceId();
        c.output(lineage, KV.of(key, "Day [" + output.getDay() + "] was copied from input[something]"));
        c.output(lineage, KV.of(key, "Software version v0.12"));
        c.output(lineage, KV.of(key, "I am a random UUID[" + UUID.randomUUID() + "]"));
      }

    })
                                                  // the main output is first, and then an additional collection for the lineage data
                                                  .withOutputTags(data, TupleTagList.of(lineage)));

    // Write the data result as an Avro file
    results.get(data).apply("Save the records as Avro", AvroIO.write(UntypedOccurrence.class).to("demo/output/data"));

    // Collect all the lineage statements for each record
    PCollection<KV<String, Iterable<String>>> finalLineage =

      results.get(lineage).apply(GroupByKey.create());

    // Transform to "JSON-ish data"so we can save as a text file to read the output
    PCollection<String> lineageAsJSON = finalLineage.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        // worst code follows - demo only!
        KV<String, Iterable<String>> record = c.element();
        StringBuilder recordAsJSON =
          new StringBuilder("{\n" + "  \"key\":" + record.getKey() + "\n" + "  \"lineage\": [\n");
        recordAsJSON.append("    " + StreamSupport.stream(record.getValue().spliterator(), false)
          .collect(Collectors.joining(",\n    ")));
        recordAsJSON.append("\n  ]\n}\n");
        c.output(recordAsJSON.toString());
      }
    }));

    lineageAsJSON.apply("Save the records as Avro",
                        TextIO.write().to("demo/output/lineage.json")); // not valid JSON but for demo...

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }
}
