package org.gbif.pipelines.transforms.core;

import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class TemporalRecordTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static class CleanDateCreate extends DoFn<TemporalRecord, TemporalRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      TemporalRecord tr = TemporalRecord.newBuilder(context.element()).build();
      tr.setCreated(0L);
      context.output(tr);
    }
  }

  @Test
  public void transformationTest() {
    // State
    ExtendedRecord record = ExtendedRecord.newBuilder().setId("0").build();
    record.getCoreTerms().put(DwcTerm.year.qualifiedName(), "1999");
    record.getCoreTerms().put(DwcTerm.month.qualifiedName(), "2");
    record.getCoreTerms().put(DwcTerm.day.qualifiedName(), "2");
    record.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), " 1999-02-02 ");
    record.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1999-02-02T12:26");
    record.getCoreTerms().put(DcTerm.modified.qualifiedName(), "1999-02-02T12:26");
    final List<ExtendedRecord> input = Collections.singletonList(record);

    // Expected
    final List<TemporalRecord> dataExpected =
        createTemporalRecordList(
            1999, 2, 2, 33, 33, LocalDate.of(1999, 2, 2), LocalDateTime.of(1999, 2, 2, 12, 26));

    // When
    PCollection<TemporalRecord> dataStream =
        p.apply(Create.of(input))
            .apply(TemporalTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(dataExpected);
    p.run();
  }

  @Test
  public void transformationDateMonthTest() {
    // State
    ExtendedRecord record = ExtendedRecord.newBuilder().setId("0").build();
    record.getCoreTerms().put(DwcTerm.year.qualifiedName(), "1999");
    record.getCoreTerms().put(DwcTerm.month.qualifiedName(), "2");
    record.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "1999-02");
    record.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "1999-02");
    record.getCoreTerms().put(DcTerm.modified.qualifiedName(), "1999-02");
    final List<ExtendedRecord> input = Collections.singletonList(record);

    // Expected
    final List<TemporalRecord> dataExpected =
        createTemporalRecordList(
            1999, 2, null, null, null, YearMonth.of(1999, 2), YearMonth.of(1999, 2));

    // When
    PCollection<TemporalRecord> dataStream =
        p.apply(Create.of(input))
            .apply(TemporalTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(dataExpected);
    p.run();
  }

  @Test
  public void emptyErTest() {

    // State
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();

    // When
    PCollection<TemporalRecord> dataStream =
        p.apply(Create.of(er))
            .apply(TemporalTransform.builder().create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).empty();
    p.run();
  }

  @Test
  public void dmyTransformationTest() {
    // State
    final List<ExtendedRecord> input = new ArrayList<>();

    ExtendedRecord record = ExtendedRecord.newBuilder().setId("0").build();
    record.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), "01/02/1999T12:26Z");
    record.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), "01/04/1999");
    record.getCoreTerms().put(DcTerm.modified.qualifiedName(), "01/03/1999T12:26");
    input.add(record);
    // Expected
    TemporalRecord expected =
        TemporalRecord.newBuilder()
            .setId("0")
            .setYear(1999)
            .setMonth(2)
            .setDay(1)
            .setStartDayOfYear(32)
            .setEndDayOfYear(32)
            .setEventDate(
                EventDate.newBuilder()
                    .setGte("1999-02-01T12:26")
                    .setLte("1999-02-01T12:26")
                    .build())
            .setDateIdentified("1999-04-01")
            .setModified("1999-03-01T12:26")
            .setCreated(0L)
            .build();

    // When
    PCollection<TemporalRecord> dataStream =
        p.apply(Create.of(input))
            .apply(
                TemporalTransform.builder()
                    .orderings(Arrays.asList(DMY_FORMATS))
                    .create()
                    .interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(Collections.singletonList(expected));
    p.run();
  }

  private List<TemporalRecord> createTemporalRecordList(
      Integer year,
      Integer month,
      Integer day,
      Integer sDoY,
      Integer eDoY,
      Temporal eventDate,
      Temporal other) {
    return Collections.singletonList(
        TemporalRecord.newBuilder()
            .setId("0")
            .setYear(year)
            .setMonth(month)
            .setDay(day)
            .setStartDayOfYear(sDoY)
            .setEndDayOfYear(eDoY)
            .setEventDate(
                EventDate.newBuilder()
                    .setGte(eventDate.toString())
                    .setLte(eventDate.toString())
                    .build())
            .setDateIdentified(other.toString())
            .setModified(other.toString())
            .setCreated(0L)
            .build());
  }
}
