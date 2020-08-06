package org.gbif.pipelines.transforms.core;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.parsers.temporal.ParsedTemporal;
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
    final List<ExtendedRecord> input = createExtendedRecordList("1999-01-01T12:26Z");

    // Expected
    // First
    final LocalDateTime fromOne = LocalDateTime.of(1999, 1, 1, 12, 26);
    final ParsedTemporal periodOne = ParsedTemporal.create();
    periodOne.setFromDate(fromOne);
    periodOne.setYear(Year.of(1999));
    periodOne.setMonth(Month.of(1));
    periodOne.setDay(1);
    periodOne.setDay(1);
    periodOne.setDay(1);

    final List<TemporalRecord> dataExpected = createTemporalRecordList(periodOne);

    // When
    PCollection<TemporalRecord> dataStream =
        p.apply(Create.of(input))
            .apply(TemporalTransform.create().interpret())
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
            .apply(TemporalTransform.create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).empty();
    p.run();
  }

  private List<ExtendedRecord> createExtendedRecordList(String... events) {
    return Arrays.stream(events)
        .map(
            x -> {
              ExtendedRecord record = ExtendedRecord.newBuilder().setId("0").build();
              record.getCoreTerms().put(DwcTerm.year.qualifiedName(), "1999");
              record.getCoreTerms().put(DwcTerm.month.qualifiedName(), "1");
              record.getCoreTerms().put(DwcTerm.day.qualifiedName(), "1");
              record.getCoreTerms().put(DwcTerm.eventDate.qualifiedName(), x);
              record.getCoreTerms().put(DwcTerm.dateIdentified.qualifiedName(), x);
              record.getCoreTerms().put(DcTerm.modified.qualifiedName(), x);
              return record;
            })
        .collect(Collectors.toList());
  }

  private List<TemporalRecord> createTemporalRecordList(ParsedTemporal... dates) {
    return Arrays.stream(dates)
        .map(
            x -> {
              String from = x.getFromOpt().map(Temporal::toString).orElse(null);
              String to = x.getToOpt().map(Temporal::toString).orElse(null);
              return TemporalRecord.newBuilder()
                  .setId("0")
                  .setYear(x.getYearOpt().map(Year::getValue).orElse(null))
                  .setMonth(x.getMonthOpt().map(Month::getValue).orElse(null))
                  .setDay(x.getDayOpt().orElse(null))
                  .setEventDate(EventDate.newBuilder().setGte(from).setLte(to).build())
                  .setDateIdentified(x.getFromOpt().map(Temporal::toString).orElse(null))
                  .setModified(x.getFromOpt().map(Temporal::toString).orElse(null))
                  .setCreated(0L)
                  .build();
            })
        .collect(Collectors.toList());
  }
}
