package org.gbif.pipelines.transform;

import org.gbif.pipelines.core.functions.interpretation.error.IssueLineageRecord;
import org.gbif.pipelines.interpretation.ExtendedRecordInterpreter;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * This transform provides interpretation for the record level fields: basisOfRecord, sex,
 * individualCount, establishmentMeans and lifeStage.
 */
public class InterpretedExtendedRecordTransform extends RecordTransform<ExtendedRecord, InterpretedExtendedRecord> {

  public InterpretedExtendedRecordTransform() {
    super("Interpreting record level terms");
  }

  /**
   * Transforms a ExtendedRecord into a InterpretedExtendedRecord.
   */
  @Override
  DoFn<ExtendedRecord, KV<String, InterpretedExtendedRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, InterpretedExtendedRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        // Context element to be interpreted
        ExtendedRecord record = context.element();

        // Transformation main output
        InterpretedExtendedRecord interpretedRecord = InterpretedExtendedRecord.newBuilder().setId(record.getId()).build();

        final IssueLineageRecord issueLineageRecord = Interpretation.of(record)
          .using(ExtendedRecordInterpreter.interpretBasisOfRecord(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretSex(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretEstablishmentMeans(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretLifeStage(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretTypeStatus(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretIndividualCount(interpretedRecord))
          .getIssueLineageRecord(record.getId());

        // Additional outputs
        context.output(getIssueTag(), KV.of(interpretedRecord.getId(), issueLineageRecord));

        // Main output
        context.output(getDataTag(), KV.of(interpretedRecord.getId(), interpretedRecord));
      }
    };
  }
}
