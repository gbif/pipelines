package org.gbif.pipelines.indexing;

import org.gbif.pipelines.builder.SolrDocBuilder;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.FunctionFactory;
import org.gbif.pipelines.core.functions.descriptor.CustomTypeDescriptors;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrenceLowerCase;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.solr.SolrIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline that reads an Avro file and indexes it into Elastic Search.
 * <p>
 * TODO: A lot of hard coded stuff here to sort out...
 */
public class Avro2SolrPipeline extends AbstractSparkOnYarnPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(Avro2SolrPipeline.class);

    /*
    private static final String SOURCE_PATH = "hdfs://ha-nn/user/hive/warehouse/tim.db/occ_all/*";
    private static final String[] ES_HOSTS =
      new String[] {"http://c4n1.gbif.org:9200",
        "http://c4n2.gbif.org:9200",
        "http://c4n3.gbif.org:9200",
        "http://c4n4.gbif.org:9200",
        "http://c4n5.gbif.org:9200",
        "http://c4n6.gbif.org:9200",
        "http://c4n7.gbif.org:9200",
        "http://c4n8.gbif.org:9200",
        "http://c4n9.gbif.org:9200"};
    */
    private static final String SOURCE_PATH = "hdfs://ha-nn/user/hive/warehouse/occ_all/*";
    private static final String SOLR_HOST = "c3master1-vh.gbif.org:2181,c3master2-vh.gbif.org:2181,c3master3-vh.gbif.org:2181/solr5c2";

    public static void main(String[] args) {

        Configuration conf = new Configuration(); // assume defaults on CP
        Pipeline p = newPipeline(args, conf);
        Coders.registerAvroCoders(p, UntypedOccurrenceLowerCase.class, TypedOccurrence.class);

        // Read Avro files
        PCollection<UntypedOccurrenceLowerCase> verbatimRecords = p.apply(
                "Read Avro files", AvroIO.read(UntypedOccurrenceLowerCase.class).from(SOURCE_PATH));

        // Convert the objects (interpretation)
        PCollection<TypedOccurrence> interpreted = verbatimRecords.apply("Interpret occurrence records"
          , MapElements.into(CustomTypeDescriptors.typedOccurrencies()).via(FunctionFactory.interpretOccurrenceLowerCase()::apply));

        // Do the nub lookup
        PCollection<TypedOccurrence> matched = interpreted.apply("Align to backbone using species/match"
          , MapElements.into(CustomTypeDescriptors.typedOccurrencies()).via(FunctionFactory.gbifSpeciesMatch()::apply));

        // Write the file to SOLR
        final SolrIO.ConnectionConfiguration conn = SolrIO.ConnectionConfiguration
                .create(SOLR_HOST);

        PCollection<SolrInputDocument> inputDocs = matched.apply(
                "Convert to SOLR", ParDo.of(new SolrDocBuilder()));

        inputDocs.apply(SolrIO.write().to("beam-demo1").withConnectionConfiguration(conn));

        // instruct the writer to use a provided document ID
        LOG.info("Starting the pipeline");
        PipelineResult result = p.run();
        result.waitUntilFinish();
        LOG.info("Pipeline finished with state: {} ", result.getState());
    }
}
