package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.Validation;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
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
  public DoFn<ExtendedRecord, KV<String, InterpretedExtendedRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, InterpretedExtendedRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        // Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();
        List<Validation> validations = new ArrayList<>();

        // Transformation main output
        InterpretedExtendedRecord interpretedRecord = InterpretedExtendedRecord.newBuilder().setId(id).build();

        Interpretation.of(extendedRecord)
          .using(ExtendedRecordInterpreter.interpretBasisOfRecord(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretSex(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretEstablishmentMeans(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretLifeStage(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretTypeStatus(interpretedRecord))
          .using(ExtendedRecordInterpreter.interpretIndividualCount(interpretedRecord))
          .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        //additional outputs
        if (!validations.isEmpty()) {
          OccurrenceIssue issue = OccurrenceIssue.newBuilder().setId(id).setIssues(validations).build();
          context.output(getIssueTag(), KV.of(id, issue));
        }

        //main output
        context.output(getDataTag(), KV.of(id, interpretedRecord));
      }
    };
  }

  @Override
  public InterpretedExtendedRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(pipeline, OccurrenceIssue.class, InterpretedExtendedRecord.class, ExtendedRecord.class);
    return this;
  }

}
