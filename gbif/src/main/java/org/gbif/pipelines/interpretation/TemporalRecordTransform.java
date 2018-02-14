package org.gbif.pipelines.interpretation;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceIssue;
import org.gbif.pipelines.io.avro.TemporalRecord;
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

public class TemporalRecordTransform extends PTransform<PCollection<ExtendedRecord>, PCollectionTuple> {

  private static final String FILTER_STEP = "Interpret temporal record";

  private final TupleTag<TemporalRecord> dataTag = new TupleTag<TemporalRecord>() {};
  private final TupleTag<OccurrenceIssue> issueTag = new TupleTag<OccurrenceIssue>() {};

  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {
    return input.apply(FILTER_STEP, ParDo.of(interpret()).withOutputTags(dataTag, TupleTagList.of(issueTag)));
  }

  /**
   * Transforms a ExtendedRecord into a InterpretedExtendedRecord.
   */
  private DoFn<ExtendedRecord, TemporalRecord> interpret() {
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
          context.output(issueTag, OccurrenceIssue.newBuilder().setId(extendedRecord.getId()).setIssues(validations).build());
        }

        //main output
        context.output(dataTag, temporalRecord);
      }

    };
  }

  /**
   * @return all data, without and with issues
   */
  public TupleTag<TemporalRecord> getDataTag() {
    return dataTag;
  }

  /**
   * @return data only with issues
   */
  public TupleTag<OccurrenceIssue> getIssueTag() {
    return issueTag;
  }

  /**
   * Translates a OccurrenceIssue into Validation object.
   */
  private static Validation toValidation(org.gbif.api.vocabulary.OccurrenceIssue occurrenceIssue) {
    return Validation.newBuilder()
      .setName(occurrenceIssue.name())
      .setSeverity(occurrenceIssue.getSeverity().name())
      .build();
  }
}


