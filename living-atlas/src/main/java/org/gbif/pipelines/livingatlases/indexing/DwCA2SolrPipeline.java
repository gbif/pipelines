package org.gbif.pipelines.livingatlases.indexing;

import org.gbif.pipelines.core.beam.BeamFunctions;
import org.gbif.pipelines.core.beam.Coders;
import org.gbif.pipelines.core.beam.DwCAIO;
import org.gbif.pipelines.core.functions.Functions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.solr.common.SolrInputDocument;

/**
 * A demonstration for showing how to index a DwC-A into SOLR.
 * TODO: Remove hard coded things
 */
public class DwCA2SolrPipeline {

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
      "Convert the objects into untyped DwC style records",
      ParDo.of(BeamFunctions.beamify(Functions.untypedOccurrenceBuilder())))
                                                               .setCoder(AvroCoder.of(UntypedOccurrence.class));

    // Write the file to SOLR
    final SolrIO.ConnectionConfiguration conn = SolrIO.ConnectionConfiguration
      .create("c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181/solr5dev");

    PCollection<SolrInputDocument> inputDocs = verbatimRecords.apply(
      "Convert to SOLR", ParDo.of(new SolrDocBuilder()));

    inputDocs.apply(SolrIO.write().to("test-solr").withConnectionConfiguration(conn));

    PipelineResult result = p.run();
    // Note: can read result state here (e.g. a web app polling for metrics would do this)
    result.waitUntilFinish();
  }
}
