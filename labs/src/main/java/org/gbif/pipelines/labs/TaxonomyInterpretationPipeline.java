package org.gbif.pipelines.labs;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.OptionsKeyEnum;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.transform.common.Kv2Value;
import org.gbif.pipelines.transform.record.TaxonRecordTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.config.DataPipelineOptionsFactory.createDefaultTaxonOptions;

/**
 * A simple demonstration showing a pipeline running locally which will read UntypedOccurrence from a DwC-A file and
 * save the result as an Avro file.
 */
public class TaxonomyInterpretationPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(TaxonomyInterpretationPipeline.class);

  private static final String SOURCE_PATH = "hdfs://ha-nn/pipelines/avrotest1/raw/*";
  private static final String TAXON_OUT_PATH_DIR = "hdfs://ha-nn/pipelines/avrotest1/taxon/";
  private static final String ISSUES_OUT_PATH_DIR = "hdfs://ha-nn/pipelines/avrotest1/taxonIssues/";

  /**
   * Suitable to run from command line.
   */
  public static void main(String[] args) {
    Configuration config = new Configuration();
    runPipeline(createDefaultTaxonOptions(config, SOURCE_PATH, TAXON_OUT_PATH_DIR, ISSUES_OUT_PATH_DIR, args));
  }

  /**
   * Suitable to run programmatically.
   */
  public static void runPipelineProgrammatically(DataProcessingPipelineOptions options) {
    runPipeline(options);
  }

  private static void runPipeline(DataProcessingPipelineOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    // register Avro coders for serializing our messages
    Coders.registerAvroCoders(pipeline, ExtendedRecord.class, TaxonRecord.class, OccurrenceIssue.class);

    // when we create a schema from a java class it does not implement SpecificRecord
    pipeline.getCoderRegistry().registerCoderForClass(TaxonRecord.class, AvroCoder.of(TaxonRecord.class));

    // Read Avro files
    PCollection<ExtendedRecord> verbatimRecords =
      pipeline.apply("Read Avro files", AvroIO.read(ExtendedRecord.class).from(options.getInputFile()))
        .setCoder(AvroCoder.of(ExtendedRecord.class));

    // taxon interpretation
    TaxonRecordTransform taxonRecordTransform = new TaxonRecordTransform();
    PCollectionTuple taxonomicInterpreted = verbatimRecords.apply(taxonRecordTransform);

    // write taxon records
    taxonomicInterpreted.get(taxonRecordTransform.getDataTag())
      .apply(Kv2Value.create())
      .setCoder(AvroCoder.of(TaxonRecord.class))
      .apply("Save the taxon records as Avro",
             AvroIO.write(TaxonRecord.class)
               .to(options.getTargetPaths().get(OptionsKeyEnum.GBIF_BACKBONE).filePath())
               .withTempDirectory(FileSystems.matchNewResource(options.getHdfsTempLocation(), true)));

    // write issues
    taxonomicInterpreted.get(taxonRecordTransform.getIssueTag())
      .apply(Kv2Value.create())
      .setCoder(AvroCoder.of(OccurrenceIssue.class))
      .apply("Save the taxon records as Avro",
             AvroIO.write(OccurrenceIssue.class)
               .to(options.getTargetPaths().get(OptionsKeyEnum.ISSUES).filePath())
               .withTempDirectory(FileSystems.matchNewResource(options.getHdfsTempLocation(), true)));

    LOG.info("Starting the pipeline");
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
