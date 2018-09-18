package org.gbif.pipelines.transforms;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.parsers.parsers.temporal.ParsedTemporal;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.Year;
import java.time.YearMonth;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TemporalRecordTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void transformationTest() {
    // State
    final List<ExtendedRecord> input =
        createExtendedRecordList("1999-04-17T12:26Z/12:52:17Z", "1999-04/2010-05", "2010/2011");

    // Expected
    // First
    final LocalDateTime fromOne = LocalDateTime.of(1999, 4, 17, 12, 26);
    final LocalDateTime toOne = LocalDateTime.of(1999, 4, 17, 12, 52, 17);
    final ParsedTemporal periodOne = new ParsedTemporal(fromOne, toOne);
    periodOne.setYear(Year.of(1999));
    periodOne.setMonth(Month.of(10));
    periodOne.setDay(1);
    // Second
    final YearMonth fromTwo = YearMonth.of(1999, 4);
    final YearMonth toTwo = YearMonth.of(2010, 5);
    final ParsedTemporal periodTwo = new ParsedTemporal(fromTwo, toTwo);
    periodTwo.setYear(Year.of(1999));
    periodTwo.setMonth(Month.of(10));
    periodTwo.setDay(1);
    // Third
    final Year fromThree = Year.of(2010);
    final Year toThree = Year.of(2011);
    final ParsedTemporal periodThree = new ParsedTemporal(fromThree, toThree);
    periodThree.setYear(Year.of(1999));
    periodThree.setMonth(Month.of(10));
    periodThree.setDay(1);

    final List<TemporalRecord> dataExpected =
        createTemporalRecordList(periodOne, periodTwo, periodThree);

    // When
    PCollection<TemporalRecord> dataStream =
        p.apply(Create.of(input)).apply(RecordTransforms.temporal());

    // Should
    PAssert.that(dataStream).containsInAnyOrder(dataExpected);
    p.run();
  }

  private List<ExtendedRecord> createExtendedRecordList(String... events) {
    return Arrays.stream(events)
        .map(
            x -> {
              ExtendedRecord record = ExtendedRecord.newBuilder().setId("0").build();
              record.getCoreTerms().put(DwcTerm.year.qualifiedName(), "1999");
              record.getCoreTerms().put(DwcTerm.month.qualifiedName(), "10");
              record.getCoreTerms().put(DwcTerm.day.qualifiedName(), "1");
              record.getCoreTerms().put(DwcTerm.endDayOfYear.qualifiedName(), "365");
              record.getCoreTerms().put(DwcTerm.startDayOfYear.qualifiedName(), "1");
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
              String from = x.getFrom().map(Temporal::toString).orElse(null);
              String to = x.getTo().map(Temporal::toString).orElse(null);
              return TemporalRecord.newBuilder()
                  .setId("0")
                  .setYear(x.getYear().map(Year::getValue).orElse(null))
                  .setMonth(x.getMonth().map(Month::getValue).orElse(null))
                  .setDay(x.getDay().orElse(null))
                  .setEventDate(EventDate.newBuilder().setGte(from).setLte(to).build())
                  .setDateIdentified(x.getFrom().map(Temporal::toString).orElse(null))
                  .setModified(x.getFrom().map(Temporal::toString).orElse(null))
                  .setStartDayOfYear(1)
                  .setEndDayOfYear(365)
                  .build();
            })
        .collect(Collectors.toList());
  }
}
