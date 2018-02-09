package org.gbif.pipelines.interpretation;


import org.gbif.pipelines.interpretation.parsers.TemporalInterpreterFunction;
import org.gbif.pipelines.interpretation.parsers.temporal.ParsedTemporalPeriod;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.TypedOccurrence;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

@DefaultCoder(AvroCoder.class)
public class IterpretTemporalRecord extends PTransform<PCollection<TypedOccurrence>, PCollectionTuple> {

  private static final String FILTER_STEP = "Interpret temporal";

  private final TupleTag<TemporalRecord> dataTag = new TupleTag<TemporalRecord>() {};
  private final TupleTag<TemporalRecord> issueTag = new TupleTag<TemporalRecord>() {};

  @Override
  public PCollectionTuple expand(PCollection<TypedOccurrence> input) {

    //Convert from list to map where, key - occurrenceId, value - object instance
    return input.apply(FILTER_STEP, ParDo.of(new DoFn<TypedOccurrence, TemporalRecord>() {
      @ProcessElement
      public void processElement(ProcessContext c) {

        TypedOccurrence typedOccurrence = c.element();

        Integer year = typedOccurrence.getYear();
        Integer month = typedOccurrence.getMonth();
        Integer day = typedOccurrence.getDay();
        CharSequence eventDate = typedOccurrence.getEventDate();

        ParsedTemporalPeriod temporalPeriod = TemporalInterpreterFunction.apply(year, month, day, eventDate);

        TemporalRecord temporalRecord = TemporalRecord.newBuilder()
          .setId(typedOccurrence.getOccurrenceId())
          .setYear(year)
          .setMonth(month)
          .setDay(day)
          .setEventDate(temporalPeriod.toEsString())
          .build();

        c.output(dataTag, temporalRecord);

      }
    }).withOutputTags(dataTag, TupleTagList.of(issueTag)));

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
  public TupleTag<TemporalRecord> getIssueTag() {
    return issueTag;
  }
}


