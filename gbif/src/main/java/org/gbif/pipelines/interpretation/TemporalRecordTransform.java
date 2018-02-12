package org.gbif.pipelines.interpretation;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.interpretation.parsers.temporal.ParsedTemporalDates;
import org.gbif.pipelines.interpretation.parsers.temporal.TemporalInterpreterFunction;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.Optional;
import java.util.function.BiFunction;

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
public class TemporalRecordTransform extends PTransform<PCollection<ExtendedRecord>, PCollectionTuple> {

  private static final String FILTER_STEP = "Interpret temporal record";
  private static final BiFunction<ExtendedRecord, DwcTerm, String> FUNCTION =
    (record, dwcTerm) -> Optional.ofNullable(record.getCoreTerms().get(dwcTerm.qualifiedName())).orElse("").toString();

  private final TupleTag<TemporalRecord> dataTag = new TupleTag<TemporalRecord>() {};
  private final TupleTag<TemporalRecord> issueTag = new TupleTag<TemporalRecord>() {};

  @Override
  public PCollectionTuple expand(PCollection<ExtendedRecord> input) {

    //Convert from list to map where, key - occurrenceId, value - object instance
    return input.apply(FILTER_STEP, ParDo.of(new DoFn<ExtendedRecord, TemporalRecord>() {
      @ProcessElement
      public void processElement(ProcessContext c) {

        ExtendedRecord record = c.element();

        String rawYear = FUNCTION.apply(record, DwcTerm.year);
        String rawMonth = FUNCTION.apply(record, DwcTerm.month);
        String rawDay = FUNCTION.apply(record, DwcTerm.day);
        String rawEventDate = FUNCTION.apply(record, DwcTerm.eventDate);

        ParsedTemporalDates temporalDates = TemporalInterpreterFunction.apply(rawYear, rawMonth, rawDay, rawEventDate);
        Integer year = temporalDates.getYear().map(Year::getValue).orElse(null);
        Integer month = temporalDates.getMonth().map(Month::getValue).orElse(null);
        Integer day = temporalDates.getDay().orElse(null);
        String from = temporalDates.getFrom().map(Temporal::toString).orElse(null);
        String to = temporalDates.getTo().map(Temporal::toString).orElse(null);
        //TODO: IMPROVE
        String eventDay = String.join("||", from, to);

        TemporalRecord temporalRecord = TemporalRecord.newBuilder()
          .setId(record.getId())
          .setYear(year)
          .setMonth(month)
          .setDay(day)
          .setEventDate(eventDay)
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


