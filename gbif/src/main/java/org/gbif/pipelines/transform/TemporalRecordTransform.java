package org.gbif.pipelines.transform;

import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.interpretation.TemporalRecordInterpreter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * This transformation provides interpretation for the fields: year, month, day and eventDate
 */
public class TemporalRecordTransform extends RecordTransform<ExtendedRecord, TemporalRecord> {

  public TemporalRecordTransform() {
    super("Interpret temporal record");
  }

  /**
   * Transforms a ExtendedRecord into a TemporalRecord.
   */
  @Override
  DoFn<ExtendedRecord, TemporalRecord> interpret() {
    return new DoFn<ExtendedRecord, TemporalRecord>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        //Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();
        Collection<Validation> validations = new ArrayList<>();

        //Transformation main output
        TemporalRecord temporalRecord = TemporalRecord.newBuilder().setId(extendedRecord.getId()).build();

        Interpretation.of(extendedRecord)
          .using(TemporalRecordInterpreter.interpretTemporal(temporalRecord))
          .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        //additional outputs
        if (!validations.isEmpty()) {
          context.output(getIssueTupleTag(), OccurrenceIssue.newBuilder().setId(extendedRecord.getId()).setIssues(validations).build());
        }

        //main output
        context.output(getDataTupleTag(), temporalRecord);
      }
    };
  }
}


