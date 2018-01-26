package org.gbif.pipelines.indexing;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.functions.interpretation.ExtendedRecordTransform;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.pipelines.core.functions.interpretation.InterpretationTupleTags.INTERPRETED_EXTENDED_RECORD_TUPLE_TAG;
import static org.gbif.pipelines.core.functions.interpretation.InterpretationTupleTags.OCCURRENCE_ISSUE_TUPLE_TAG;

/**
 * Reads an DwC-A Avro file containing ExtendedRecords element and performs an interpretation of basis fields.
 * The result is stored in a set of Avro files that follows the schema {@link InterpretedExtendedRecord}.
 */
public class InterpretDwCAvroPipeline extends AbstractSparkOnYarnPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretDwCAvroPipeline.class);

  private static final String SOURCE_PATH = "hdfs://ha-nn/pipelines/avrotest1/raw/*";
  private static final String INTERPRETED_OUT_PATH = "hdfs://ha-nn/pipelines/avrotest1/interpreted/";
  private static final String ISSUES_OUT_PATH = "hdfs://ha-nn/pipelines/avrotest1/issues/";

  public static void main(String[] args) {
    Configuration conf = new Configuration(); // assume defaults on CP
    Pipeline p = newPipeline(args, conf);
    Coders.registerAvroCoders(p, ExtendedRecord.class, InterpretedExtendedRecord.class);

    // Read Avro files
    PCollection<ExtendedRecord> verbatimRecords = p.apply(
      "Read Avro files", AvroIO.read(ExtendedRecord.class).from(SOURCE_PATH))
      .setCoder(AvroCoder.of(ExtendedRecord.class));

    // Convert the objects (interpretation)
    PCollectionTuple interpreted = verbatimRecords.apply(new ExtendedRecordTransform());

    interpreted.get(INTERPRETED_EXTENDED_RECORD_TUPLE_TAG)
      .setCoder(AvroCoder.of(InterpretedExtendedRecord.class))
      .apply("Write Interpreted Avro files", AvroIO.write(InterpretedExtendedRecord.class).to(INTERPRETED_OUT_PATH));
    interpreted.get(OCCURRENCE_ISSUE_TUPLE_TAG)
      .setCoder(AvroCoder.of(OccurrenceIssue.class))
      .apply("Write Interpretation Issues Avro files", AvroIO.write(OccurrenceIssue.class).to(ISSUES_OUT_PATH));

    // instruct the writer to use a provided document ID
    LOG.info("Starting interpretation the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }

}
