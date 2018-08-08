package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.InterpreterHandler;
import org.gbif.pipelines.core.interpretation.MultimediaInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.transform.RecordTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static org.gbif.pipelines.core.interpretation.MultimediaInterpreter.interpretId;
import static org.gbif.pipelines.core.interpretation.MultimediaInterpreter.interpretMultimedia;

/**
 * {@link org.apache.beam.sdk.transforms.PTransform} that runs the {@link MultimediaInterpreter}.
 */
public class MultimediaRecordTransform extends RecordTransform<ExtendedRecord, MultimediaRecord> {

  private MultimediaRecordTransform() {
    super("Interpret multimedia record");
  }

  public static MultimediaRecordTransform create() {
    return new MultimediaRecordTransform();
  }

  @Override
  public DoFn<ExtendedRecord, KV<String, MultimediaRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, MultimediaRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {
        // get the record
        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();

        // Interpret multimedia terms and add validations
        InterpreterHandler.of(extendedRecord, new MultimediaRecord())
            .withId(id)
            .using(interpretId())
            .using(interpretMultimedia())
            .consumeData(d -> context.output(getDataTag(), KV.of(id, d)))
            .consumeIssue(i -> context.output(getIssueTag(), KV.of(id, i)));
      }
    };
  }

  @Override
  public MultimediaRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline, OccurrenceIssue.class, MultimediaRecord.class, ExtendedRecord.class);
    return this;
  }
}
