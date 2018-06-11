package org.gbif.pipelines.indexing.record;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.transform.Kv2Value;
import org.gbif.pipelines.transform.record.LocationRecordTransform;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Location2AvroPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(Location2AvroPipeline.class);

  private static final String READ_STEP = "Read Avro files";
  private static final String WRITE_STEP = "Write Avro files";

  public static void main(String[] args) {

    // Create a pipeline
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    String inputFile = options.getInputFile();
    String targetDirectory = options.getDefaultTargetDirectory();

    Pipeline p = Pipeline.create(options);

    // Transforms to use
    UniqueOccurrenceIdTransform uniquenessTransform = UniqueOccurrenceIdTransform.create().withAvroCoders(p);
    LocationRecordTransform locationTransform = LocationRecordTransform.create().withAvroCoders(p);

    // STEP 1: Read Avro files
    PCollection<ExtendedRecord> verbatimRecords = p.apply(READ_STEP, AvroIO.read(ExtendedRecord.class).from(inputFile));

    // STEP 2: Validate ids uniqueness
    PCollectionTuple uniqueTuple = verbatimRecords.apply(uniquenessTransform);
    PCollection<ExtendedRecord> extendedRecords = uniqueTuple.get(uniquenessTransform.getDataTag());

    // STEP 3: Run the main transform
    PCollectionTuple locationRecordTuple = extendedRecords.apply(locationTransform);
    PCollection<LocationRecord> locationRecords = locationRecordTuple.get(locationTransform.getDataTag())
      .apply(Kv2Value.create());

    // STEP 4: Save to an avro file
    locationRecords.apply(WRITE_STEP, AvroIO.write(LocationRecord.class).to(targetDirectory));

    // Run
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
