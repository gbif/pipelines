package org.gbif.pipelines.transform;

import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.TemporalRecordInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * This transform provides interpretation for the fields: year, month, day and eventDate
 */
public class TemporalRecordTransform extends RecordTransform<ExtendedRecord, TemporalRecord> {

  public TemporalRecordTransform() {
    super("Interpret temporal record");
  }

  /**
   * Transforms a ExtendedRecord into a TemporalRecord.
   */
  @Override
  DoFn<ExtendedRecord, KV<String, TemporalRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, TemporalRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        // Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();

        // Transformation main output
        TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(extendedRecord.getId()).build();

        final IssueLineageRecord issueLineageRecord = Interpretation.of(extendedRecord)
          .using(TemporalRecordInterpreter.interpretTemporal(temporalRecord))
          .getIssueLineageRecord(extendedRecord.getId());

        // Additional output
        context.output(getIssueTag(), KV.of(temporalRecord.getId(), issueLineageRecord));

        // Main output
        context.output(getDataTag(), KV.of(temporalRecord.getId(), temporalRecord));
      }
    };
  }
}


