package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.InterpreterHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;
import org.gbif.pipelines.transform.RecordTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static org.gbif.pipelines.core.interpretation.TemporalRecordInterpreter.interpretDateIdentified;
import static org.gbif.pipelines.core.interpretation.TemporalRecordInterpreter.interpretDayOfYear;
import static org.gbif.pipelines.core.interpretation.TemporalRecordInterpreter.interpretEventDate;
import static org.gbif.pipelines.core.interpretation.TemporalRecordInterpreter.interpretId;
import static org.gbif.pipelines.core.interpretation.TemporalRecordInterpreter.interpretModifiedDate;

/** This transform provides interpretation for the fields: year, month, day and eventDate */
public class TemporalRecordTransform extends RecordTransform<ExtendedRecord, TemporalRecord> {

  private TemporalRecordTransform() {
    super("Interpret temporal record");
  }

  public static TemporalRecordTransform create() {
    return new TemporalRecordTransform();
  }

  /** Transforms a ExtendedRecord into a TemporalRecord. */
  @Override
  public DoFn<ExtendedRecord, KV<String, TemporalRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, TemporalRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        // Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();

        InterpreterHandler.of(extendedRecord, new TemporalRecord())
            .withId(id)
            .using(interpretId())
            .using(interpretEventDate())
            .using(interpretDateIdentified())
            .using(interpretModifiedDate())
            .using(interpretDayOfYear())
            .consumeData(d -> context.output(getDataTag(), KV.of(id, d)))
            .consumeIssue(i -> context.output(getIssueTag(), KV.of(id, i)));
      }
    };
  }

  @Override
  public TemporalRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline, OccurrenceIssue.class, TemporalRecord.class, ExtendedRecord.class);
    return this;
  }
}
