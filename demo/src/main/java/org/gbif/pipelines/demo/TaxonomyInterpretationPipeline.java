package org.gbif.pipelines.demo;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.TaxonomicInterpretationTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple demonstration showing a pipeline running locally which will read UntypedOccurrence from a DwC-A file and
 * save the result as an Avro file.
 */
public class TaxonomyInterpretationPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpretationPipeline.class);

  private static final String SOURCE_PATH = "hdfs://ha-nn/pipelines/avrotest1/raw/*";
  private static final String TAXON_OUT_PATH = "hdfs://ha-nn/pipelines/avrotest1/interpreted/taxon";
  private static final String TEMPORAL_OUT_PATH = "hdfs://ha-nn/tmp";

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class); // forced for this demo
    Pipeline p = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(p, ExtendedRecord.class, TaxonRecord.class);

    // Read the DwC-A using our custom reader
//    PCollection<ExtendedRecord> rawRecords = p.apply(
//      "Read from Darwin Core Archive", DwCAIO.Read.withPaths("demo/dwca.zip", "demo/target/tmp"));

    // Read Avro files
    PCollection<ExtendedRecord> verbatimRecords = p.apply(
      "Read Avro files", AvroIO.read(ExtendedRecord.class).from(SOURCE_PATH))
      .setCoder(AvroCoder.of(ExtendedRecord.class));

    PCollectionTuple taxonomicInterpreted = verbatimRecords.apply(new TaxonomicInterpretationTransform());

    taxonomicInterpreted.get(TaxonomicInterpretationTransform.TAXON_RECORD_TUPLE_TAG).apply(
      "Save the taxon records as Avro", AvroIO.write(TaxonRecord.class).to(TAXON_OUT_PATH).withTempDirectory
        (FileSystems.matchNewResource(TEMPORAL_OUT_PATH, true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }
}
