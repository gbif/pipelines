package org.gbif.pipelines.transform;

import org.gbif.pipelines.interpretation.ExtendedRecordInterpreter;
import org.gbif.pipelines.interpretation.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * This transformation provides interpretation for the record level fields: basisOfRecord, sex,
 * individualCount, establishmentMeans and lifeStage.
 */
public class ExtendedRecordTransform extends RecordTransform<ExtendedRecord, InterpretedExtendedRecord> {

  public ExtendedRecordTransform() {
    super("Interpreting record level terms");
  }

  /**
   * Transforms a ExtendedRecord into a InterpretedExtendedRecord.
   */
  @Override
  DoFn<ExtendedRecord, InterpretedExtendedRecord> interpret() {
    return new DoFn<ExtendedRecord, InterpretedExtendedRecord>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        //Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();
        Collection<Validation> validations = new ArrayList<>();

        //Transformation main output
        InterpretedExtendedRecord interpretedExtendedRecord = InterpretedExtendedRecord.newBuilder().setId(extendedRecord.getId()).build();

        Interpretation
          .of(extendedRecord)
          .using(ExtendedRecordInterpreter.interpretBasisOfRecord(interpretedExtendedRecord))
          .using(ExtendedRecordInterpreter.interpretSex(interpretedExtendedRecord))
          .using(ExtendedRecordInterpreter.interpretEstablishmentMeans(interpretedExtendedRecord))
          .using(ExtendedRecordInterpreter.interpretLifeStage(interpretedExtendedRecord))
          .using(ExtendedRecordInterpreter.interpretTypeStatus(interpretedExtendedRecord))
          .using(ExtendedRecordInterpreter.interpretIndividualCount(interpretedExtendedRecord))
          .forEachValidation(trace ->  validations.add(toValidation(trace.getContext())));

        //additional outputs
        if (!validations.isEmpty()) {
          context.output(getIssueTupleTag(), OccurrenceIssue.newBuilder().setId(extendedRecord.getId()).setIssues(validations).build());
        }

        //main output
        context.output(getDataTupleTag(), interpretedExtendedRecord);
      }
    };
  }
}
