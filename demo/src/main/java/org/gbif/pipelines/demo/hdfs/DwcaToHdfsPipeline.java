package org.gbif.pipelines.demo.hdfs;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.core.functions.descriptor.CustomTypeDescriptors;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipeline that transforms a Dwca file into an Avro file and writes it to HDFS.
 * <p>
 * This pipeline is intended to use Dwca files that are in your local system. To use a dwca file from HDFS
 * take a look at the pipelines in the gbif-pipelines project.
 */
public class DwcaToHdfsPipeline {

  private static final String TMP_DIR_DWCA = "tmpDwca";

  private static final Logger LOG = LoggerFactory.getLogger(DwcaToHdfsPipeline.class);

  /**
   * Main method.
   */
  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class); // forced for this demo
    Pipeline p = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(p, ExtendedRecord.class, UntypedOccurrence.class);

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords = p.apply(
      "Read from Darwin Core Archive", DwCAIO.Read.withPaths("demo/dwca.zip", "demo/target/tmp"));

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
