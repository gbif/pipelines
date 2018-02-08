package org.gbif.pipelines.livingatlases.indexing;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.solr.common.SolrInputDocument;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.common.beam.DwCAIO;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.core.functions.descriptor.CustomTypeDescriptors;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A demonstration for showing how to index a DwC-A into SOLR.
 * TODO: Remove hard coded things
 */
public class DwCA2SolrPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DwCA2SolrPipeline.class);

  private static final String SOLR_HOSTS = "c3master1-vh.gbif.org:2181,c3master2-vh.gbif.org:2181,c3master3-vh.gbif.org:2181/solr5c2";


  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class); // forced for this demo
    Pipeline p = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(p, ExtendedRecord.class, UntypedOccurrence.class);

    // Read the DwC-A using our custom reader
    PCollection<ExtendedRecord> rawRecords = p.apply(
      "Read from Darwin Core Archive", DwCAIO.Read.withPaths("/tmp/dwca-s-bryophytes-v4.1.zip", "demo/target/tmp"));

    // Convert the ExtendedRecord into an UntypedOccurrence record
    PCollection<UntypedOccurrence> verbatimRecords = rawRecords.apply(
      "Convert the objects into untyped DwC style records"
      , MapElements.into(CustomTypeDescriptors.untypedOccurrencies()).via(FunctionFactory.untypedOccurrenceBuilder()::apply));

    // Write the file to SOLR
    final SolrIO.ConnectionConfiguration conn = SolrIO.ConnectionConfiguration
      .create(SOLR_HOSTS);

    PCollection<SolrInputDocument> inputDocs = verbatimRecords.apply(
      "Convert to SOLR", ParDo.of(new SolrDocBuilder()));

    inputDocs.apply(SolrIO.write().to("beam-demo1").withConnectionConfiguration(conn));

    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }
}
