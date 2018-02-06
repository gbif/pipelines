package org.gbif.pipelines.core.function;

import org.gbif.pipelines.core.functions.IterpretTemporalRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.TypedOccurrence;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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

  //TODO: FIX ME!
  @Test
  @Category(NeedsRunner.class)
  public void Should_T_When_T() {
    //State
    final List<TypedOccurrence> input = createTypedOccurrenceList("1999-04-17T12:26Z/12:52:17Z", "1999-04/2010-05");
    final ZoneId zoneId = ZoneId.of("Z");
    final ZonedDateTime firstFrom = LocalDateTime.of(1999, 4, 17, 12, 26).atZone(zoneId);
    final ZonedDateTime secondFrom = LocalDateTime.of(1999, 4, 1, 0, 0).atZone(zoneId);

    final List<TemporalRecord> dataExpected = createTemporalRecordList(firstFrom, secondFrom);
    final List<TemporalRecord> issueExpected = createTemporalRecordList(firstFrom, secondFrom);

    ////When
    //p.getCoderRegistry().registerCoderForClass(TypedOccurrence.class, AvroCoder.of(TypedOccurrence.class));
    //p.getCoderRegistry().registerCoderForClass(TemporalRecord.class, AvroCoder.of(TemporalRecord.class));

    //PCollection<TypedOccurrence> inputStream = p.apply(Create.of(input));

    //IterpretTemporalRecord temporalRecord = new IterpretTemporalRecord();
    //PCollectionTuple tuple = inputStream.apply(temporalRecord);

    //PCollection<TemporalRecord> dataStream = tuple.get(temporalRecord.getDataTag());
    //PCollection<TemporalRecord> issueStream = tuple.get(temporalRecord.getIssueTag());

    ////Should
    //PAssert.that(dataStream).containsInAnyOrder(dataExpected);
    //PAssert.that(issueStream).containsInAnyOrder(issueExpected);
    //p.run();
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
        .setEventDate(events[0])
        .build())
      .collect(Collectors.toList());
  }

  private List<TemporalRecord> createTemporalRecordList(ZonedDateTime... dates) {
    return Arrays.stream(dates).map(x -> {
      long date = x.toLocalDate().toEpochDay();
      long time = x.toLocalTime().toNanoOfDay();
      return TemporalRecord.newBuilder().setId("0").setEventDate(date).setEventTime(time).build();
    }).collect(Collectors.toList());
  }

}
