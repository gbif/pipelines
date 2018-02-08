package org.gbif.pipelines.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.Validation;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * This transformation provides interpretation for the record level fields: basisOfRecord, sex,
 * individualCount, establishmentMeans and lifeStage.
 */
public class ExtendedRecordTransform extends PTransform<PCollection<ExtendedRecord>, PCollectionTuple> {

  private final TupleTag<InterpretedExtendedRecord> interpretedExtendedRecordTupleTag = new TupleTag<InterpretedExtendedRecord>(){};

  private final  TupleTag<org.gbif.pipelines.io.avro.OccurrenceIssue> occurrenceIssueTupleTag = new TupleTag<org.gbif.pipelines.io.avro.OccurrenceIssue>(){};

  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {
    return input.apply("Interpreting record level terms", ParDo.of(interpret())
      .withOutputTags(interpretedExtendedRecordTupleTag, TupleTagList.of(occurrenceIssueTupleTag)));
  }

  /**
   * Transforms a ExtendedRecord into a InterpretedExtendedRecord.
   */
  private DoFn<ExtendedRecord,InterpretedExtendedRecord> interpret() {
    return new DoFn<ExtendedRecord, InterpretedExtendedRecord>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        //Transformation main output
        InterpretedExtendedRecord interpretedExtendedRecord = new InterpretedExtendedRecord();

        //Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();
        Collection<Validation> validations = new ArrayList<>();

        //Id
        interpretedExtendedRecord.setId(extendedRecord.getId());

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
          context.output(getOccurrenceIssueTupleTag(),
                         org.gbif.pipelines.io.avro.OccurrenceIssue.newBuilder()
                           .setId(extendedRecord.getId())
                           .setIssues(validations).build());
        }

        //main output
        context.output(getInterpretedExtendedRecordTupleTag(), interpretedExtendedRecord);
      }


    };
  }

  public TupleTag<InterpretedExtendedRecord> getInterpretedExtendedRecordTupleTag() {
    return interpretedExtendedRecordTupleTag;
  }

  public TupleTag<org.gbif.pipelines.io.avro.OccurrenceIssue> getOccurrenceIssueTupleTag() {
    return occurrenceIssueTupleTag;
  }

  /**
   * Translates a OccurrenceIssue into Validation object.
   */
  private static Validation toValidation(OccurrenceIssue occurrenceIssue) {
    return Validation.newBuilder()
      .setName(occurrenceIssue.name())
      .setSeverity(occurrenceIssue.getSeverity().name()).build();
  }
}
