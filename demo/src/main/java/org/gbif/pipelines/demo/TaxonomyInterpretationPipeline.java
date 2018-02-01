package org.gbif.pipelines.demo;

import org.gbif.api.v2.NameUsageMatch2;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.TaxonomicInterpretationTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;

import java.util.Collections;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple demonstration showing a pipeline running locally which will read UntypedOccurrence from a DwC-A file and
 * save the result as an Avro file.
 */
public class TaxonomyInterpretationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpretationPipeline.class);

  private static final String SOURCE_PATH = "hdfs://ha-nn/pipelines/avrotest1/raw/*";
  private static final String TAXON_OUT_PATH = "hdfs://ha-nn/pipelines/avrotest1/taxon/taxon";
  private static final String ISSUES_OUT_PATH = "hdfs://ha-nn/pipelines/avrotest1/taxonIssues/taxonIssues";
  private static final String TEMPORAL_OUT_PATH = "hdfs://ha-nn/tmp";

  public static void main(String[] args) {
    Configuration config = new Configuration();
    HadoopFileSystemOptions options =
      PipelineOptionsFactory.fromArgs(args).withValidation().as(HadoopFileSystemOptions.class);
    options.setHdfsConfiguration(Collections.singletonList(config));
    options.setRunner(DirectRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class, TaxonRecord.class, OccurrenceIssue.class);

    // when we create a schema from a java class it does not implement SpecificRecord
    pipeline.getCoderRegistry().registerCoderForClass(NameUsageMatch2.class, AvroCoder.of(NameUsageMatch2.class));

    // Read Avro files
    PCollection<ExtendedRecord> verbatimRecords =
      pipeline.apply("Read Avro files", AvroIO.read(ExtendedRecord.class).from(SOURCE_PATH))
        .setCoder(AvroCoder.of(ExtendedRecord.class));

    PCollectionTuple taxonomicInterpreted = verbatimRecords.apply(new TaxonomicInterpretationTransform());

    // write taxon records
    taxonomicInterpreted.get(TaxonomicInterpretationTransform.TAXON_RECORD_TUPLE_TAG).setCoder(AvroCoder.of
      (TaxonRecord.class))
      .apply("Save the taxon records as Avro",
             AvroIO.write(TaxonRecord.class)
               .to(TAXON_OUT_PATH)
               .withTempDirectory(FileSystems.matchNewResource(TEMPORAL_OUT_PATH, true)));

    // write issues
    taxonomicInterpreted.get(TaxonomicInterpretationTransform.TAXON_ISSUES_TUPLE_TAG).setCoder(AvroCoder.of
      (OccurrenceIssue.class))
      .apply("Save the taxon records as Avro",
             AvroIO.write(OccurrenceIssue.class)
               .to(ISSUES_OUT_PATH)
               .withTempDirectory(FileSystems.matchNewResource(TEMPORAL_OUT_PATH, true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }
}
