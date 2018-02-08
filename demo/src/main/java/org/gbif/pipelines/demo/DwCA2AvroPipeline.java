package org.gbif.pipelines.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.core.functions.descriptor.CustomTypeDescriptors;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple demonstration showing a pipeline running locally which will read UntypedOccurrence from a DwC-A file and
 * save the result as an Avro file.
 */
public class DwCA2AvroPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(DwCA2AvroPipeline.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        //options.setRunner(DirectRunner.class); // forced for this demo
        Pipeline p = Pipeline.create(options);

        // register Avro coders for serializing our messages
        Coders.registerAvroCoders(p, ExtendedRecord.class, UntypedOccurrence.class);

        // Read the DwC-A using our custom reader
        PCollection<ExtendedRecord> rawRecords =
                p.apply("Read from Darwin Core Archive", DwCAIO.Read.withPaths("demo/dwca.zip", "demo/target/tmp"));

        // Convert the ExtendedRecord into an UntypedOccurrence record

        PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
                "Convert the objects into untyped DwC style records"
                , MapElements.into(CustomTypeDescriptors.untypedOccurrencies()).via(FunctionFactory.untypedOccurrenceBuilder()::apply));

        // Write the result as an Avro file
        rawRecords.apply(
                "Save the records as Avro", AvroIO.write(ExtendedRecord.class).to("demo/output/data"));

        LOG.info("Starting the pipeline");
        PipelineResult result = p.run();
        result.waitUntilFinish();
        LOG.info("Pipeline finished with state: {} ", result.getState());
    }
}
