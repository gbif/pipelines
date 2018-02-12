package org.gbif.pipelines.interpretation;

import org.gbif.pipelines.interpretation.parsers.temporal.ParsedTemporalPeriod;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.TypedOccurrence;

import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IterpretTemporalRecordTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() {
    //State
    final List<TypedOccurrence> input = createTypedOccurrenceList("1999-04-17T12:26Z/12:52:17Z", "1999-04/2010-05", "2010/2011");

    //First
    final LocalDateTime fromOne = LocalDateTime.of(1999, 4, 17, 12, 26);
    final LocalDateTime toOne = LocalDateTime.of(1999, 4, 17, 12, 52, 17);
    final ParsedTemporalPeriod periodOne = new ParsedTemporalPeriod(fromOne, toOne);
    //Second
    final YearMonth fromTwo = YearMonth.of(1999, 4);
    final YearMonth toTwo = YearMonth.of(2010, 5);
    final ParsedTemporalPeriod periodTwo = new ParsedTemporalPeriod(fromTwo, toTwo);
    //Third
    final Year fromThree = Year.of(2010);
    final Year toThree = Year.of(2011);
    final ParsedTemporalPeriod periodThree = new ParsedTemporalPeriod(fromThree, toThree);

    final List<TemporalRecord> dataExpected = createTemporalRecordList(periodOne, periodTwo, periodThree);

    //When
    p.getCoderRegistry().registerCoderForClass(TypedOccurrence.class, AvroCoder.of(TypedOccurrence.class));
    p.getCoderRegistry().registerCoderForClass(TemporalRecord.class, AvroCoder.of(TemporalRecord.class));

    PCollection<TypedOccurrence> inputStream = p.apply(Create.of(input));

    IterpretTemporalRecord temporalRecord = new IterpretTemporalRecord();
    PCollectionTuple tuple = inputStream.apply(temporalRecord);

    PCollection<TemporalRecord> dataStream = tuple.get(temporalRecord.getDataTag());

    //Should
    PAssert.that(dataStream).containsInAnyOrder(dataExpected);
    p.run();
  }

  private List<TypedOccurrence> createTypedOccurrenceList(String... events) {
    return Arrays.stream(events)
      .map(x -> TypedOccurrence.newBuilder()
        .setOccurrenceId("0")
        .setKingdomKey(0)
        .setPhylumKey(0)
        .setClassKey(0)
        .setOrderKey(0)
        .setFamilyKey(0)
        .setGenusKey(0)
        .setSpeciesKey(0)
        .setNubKey(0)
        .setDecimalLatitude(0)
        .setDecimalLongitude(0)
        .setYear(0)
        .setMonth(0)
        .setDay(0)
        .setEventDate(x)
        .build())
      .collect(Collectors.toList());
  }

  private List<TemporalRecord> createTemporalRecordList(ParsedTemporalPeriod... dates) {
    return Arrays.stream(dates)
      .map(x -> TemporalRecord.newBuilder()
        .setId("0")
        .setYear(0)
        .setMonth(0)
        .setDay(0)
        .setEventDate(x.toEsString())
        .build())
      .collect(Collectors.toList());
  }

}
