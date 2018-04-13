package org.gbif.pipelines.assembling.pipelines;

import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.transform.validator.UniqueOccurrenceIdTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

public class GbifInterpretationPipeline extends BaseInterpretationPipeline {

  private final DataProcessingPipelineOptions options;

  private GbifInterpretationPipeline(DataProcessingPipelineOptions options) {
    this.options = options;
  }

  public static GbifInterpretationPipeline newInstance(
    DataProcessingPipelineOptions options
  ) {
    return new GbifInterpretationPipeline(options);
  }

  @Override
  protected String getInput() {
    return options.getInputFile();
  }

  @Override
  protected PCollection<ExtendedRecord> beforeInterpretations(
    PCollection<ExtendedRecord> verbatimRecords, Pipeline pipeline
  ) {
    UniqueOccurrenceIdTransform uniquenessTransform = new UniqueOccurrenceIdTransform().withAvroCoders(pipeline);
    PCollectionTuple uniqueTuple = verbatimRecords.apply(uniquenessTransform);
    PCollection<ExtendedRecord> extendedRecords = uniqueTuple.get(uniquenessTransform.getDataTag());
    return extendedRecords;
  }

  @Override
  protected void otherOperations(PCollection<ExtendedRecord> verbatimRecords, Pipeline pipeline) {
    // Do nothing
  }

  @Override
  protected PipelineOptions getPipelineOptions() {
    return options;
  }
}
