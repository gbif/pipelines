package org.gbif.pipelines.indexing;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.transform.TypeDescriptors;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.config.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.transform.InterpretedExtendedRecordTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads an DwC-A Avro file containing ExtendedRecords element and performs an interpretation of basis fields.
 * The result is stored in a set of Avro files that follows the schema {@link InterpretedExtendedRecord}.
 */
public class InterpretDwCAvroPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretDwCAvroPipeline.class);

  public static void main(String[] args) {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    Pipeline p = Pipeline.create(options);
    Coders.registerAvroCoders(p, ExtendedRecord.class, InterpretedExtendedRecord.class);

    // STEP 1: Read Avro files
    PCollection<ExtendedRecord> verbatimRecords =
      p.apply("Read Avro files", AvroIO.read(ExtendedRecord.class).from(options.getInputFile()))
        .setCoder(AvroCoder.of(ExtendedRecord.class));

    // STEP 2: Convert the objects (interpretation)
    InterpretedExtendedRecordTransform transform = new InterpretedExtendedRecordTransform();
    PCollectionTuple interpreted = verbatimRecords.apply(transform);

    // STEP 3: Record level interpretations
    interpreted.get(transform.getDataTag())
      .apply(MapElements.into(TypeDescriptors.interpretedExtendedRecord()).via(KV::getValue))
      .setCoder(AvroCoder.of(InterpretedExtendedRecord.class))
      .apply("Write Interpreted Avro files",
             AvroIO.write(InterpretedExtendedRecord.class)
               .to(options.getTargetPaths().get(Interpretation.RECORD_LEVEL).getFilePath()));

    // STEP 4: Exporting issues
    interpreted.get(transform.getIssueTag())
      .apply(MapElements.into(TypeDescriptors.occurrenceIssue()).via(KV::getValue))
      .setCoder(AvroCoder.of(OccurrenceIssue.class))
      .apply("Write Interpretation Issues Avro files",
             AvroIO.write(OccurrenceIssue.class).to(options.getTargetPaths().get(Interpretation.ISSUES).getFilePath()));

    // Instruct the writer to use a provided document ID
    LOG.info("Starting interpretation the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
