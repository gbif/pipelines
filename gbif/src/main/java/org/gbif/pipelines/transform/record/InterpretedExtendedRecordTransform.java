package org.gbif.pipelines.transform.record;

import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.InterpreterHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.transform.RecordTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter.interpretBasisOfRecord;
import static org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter.interpretEstablishmentMeans;
import static org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter.interpretId;
import static org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter.interpretIndividualCount;
import static org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter.interpretLifeStage;
import static org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter.interpretReferences;
import static org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter.interpretSex;
import static org.gbif.pipelines.core.interpretation.ExtendedRecordInterpreter.interpretTypeStatus;

/**
 * This transform provides interpretation for the record level fields: basisOfRecord, sex,
 * individualCount, establishmentMeans and lifeStage.
 */
public class InterpretedExtendedRecordTransform
    extends RecordTransform<ExtendedRecord, InterpretedExtendedRecord> {

  private InterpretedExtendedRecordTransform() {
    super("Interpreting record level terms");
  }

  public static InterpretedExtendedRecordTransform create() {
    return new InterpretedExtendedRecordTransform();
  }

  /** Transforms a ExtendedRecord into a InterpretedExtendedRecord. */
  @Override
  public DoFn<ExtendedRecord, KV<String, InterpretedExtendedRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, InterpretedExtendedRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        // Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();

        InterpreterHandler.of(extendedRecord, new InterpretedExtendedRecord())
            .withId(id)
            .using(interpretId())
            .using(interpretBasisOfRecord())
            .using(interpretSex())
            .using(interpretEstablishmentMeans())
            .using(interpretLifeStage())
            .using(interpretTypeStatus())
            .using(interpretIndividualCount())
            .using(interpretReferences())
            .consumeData(d -> context.output(getDataTag(), KV.of(id, d)))
            .consumeIssue(i -> context.output(getIssueTag(), KV.of(id, i)));
      }
    };
  }

  @Override
  public InterpretedExtendedRecordTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline, OccurrenceIssue.class, InterpretedExtendedRecord.class, ExtendedRecord.class);
    return this;
  }
}
