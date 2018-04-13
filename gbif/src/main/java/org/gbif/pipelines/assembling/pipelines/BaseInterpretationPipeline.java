package org.gbif.pipelines.assembling.pipelines;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;

public abstract class BaseInterpretationPipeline implements InterpretationPipeline {

  private static final String READ_STEP = "Read Avro files";

  @Override
  public Pipeline createInterpretationPipeline(
    List<InterpretationStep> steps
  ) {
    // STEP 0: create pipeline from options
    Pipeline pipeline = Pipeline.create(getPipelineOptions());

    // STEP 1: Read Avro files
    PCollection<ExtendedRecord> verbatimRecords =
      pipeline.apply(READ_STEP, AvroIO.read(ExtendedRecord.class).from(getInput()));

    // STEP 2: Common operations before running the interpretations
    PCollection<ExtendedRecord> extendedRecords = beforeInterpretations(verbatimRecords, pipeline);

    // STEP 3: interpretations
    for (InterpretationStep interpretation : steps) {
      interpretation.appendToPipeline(extendedRecords, pipeline);
    }

    otherOperations(extendedRecords, pipeline);

    return pipeline;
  }

  protected abstract String getInput();

  protected abstract PCollection<ExtendedRecord> beforeInterpretations(
    PCollection<ExtendedRecord> verbatimRecords, Pipeline pipeline
  );

  protected abstract void otherOperations(PCollection<ExtendedRecord> verbatimRecords, Pipeline pipeline);

  protected abstract PipelineOptions getPipelineOptions();

}
