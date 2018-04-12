package org.gbif.pipelines.indexing.record;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.transform.Kv2Value;
import org.gbif.pipelines.transform.record.TemporalRecordTransform;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Temporal2AvroPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(Temporal2AvroPipeline.class);

  private static final String READ_STEP = "Read Avro files";
  private static final String WRITE_STEP = "Write Avro files";

  public static void main(String[] args) {

    // Create a pipeline
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    Pipeline p = Pipeline.create(options);
    String inputFile = options.getInputFile();
    String targetDirectory = options.getDefaultTargetDirectory();

    // Transforms to use
    UniqueOccurrenceIdTransform uniquenessTransform = UniqueOccurrenceIdTransform.create().withAvroCoders(p);
    TemporalRecordTransform temporalRecordTransform = TemporalRecordTransform.create().withAvroCoders(p);

    // STEP 1: Read Avro files
    PCollection<ExtendedRecord> verbatimRecords = p.apply(READ_STEP, AvroIO.read(ExtendedRecord.class).from(inputFile));

    // STEP 2: Validate ids uniqueness
    PCollectionTuple uniqueTuple = verbatimRecords.apply(uniquenessTransform);
    PCollection<ExtendedRecord> extendedRecords = uniqueTuple.get(uniquenessTransform.getDataTag());

    // STEP 3: Run main transform
    PCollectionTuple temporalRecordTuple = extendedRecords.apply(temporalRecordTransform);
    PCollection<TemporalRecord> temporalRecords =
      temporalRecordTuple.get(temporalRecordTransform.getDataTag()).apply(Kv2Value.create());

    // STEP 4: Save to an avro file
    temporalRecords.apply(WRITE_STEP, AvroIO.write(TemporalRecord.class).to(targetDirectory));

    // Run
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
