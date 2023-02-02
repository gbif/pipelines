package org.gbif.pipelines.transforms.core;

import java.util.Optional;
import lombok.SneakyThrows;
import org.gbif.pipelines.io.avro.EventDate;
import org.junit.Assert;
import org.junit.Test;

public class TemporalCoverageFnTest {

  @Test
  @SneakyThrows
  public void accumulatorTest() {
    // Tests an accumulator built from a collection of coordinates
    TemporalCoverageFn.Accum accum = new TemporalCoverageFn.Accum();
    accum
        .acc(EventDate.newBuilder().setGte("2000-01-01").setLte("2021-01-01").build())
        .acc(EventDate.newBuilder().setGte("2021-01-01").setLte("2021-02-01").build())
        .acc(EventDate.newBuilder().setGte("2021-01-01").setLte("2022-03-01").build());

    EventDate expected = EventDate.newBuilder().setGte("2000-01-01").setLte("2022-03-01").build();

    Optional<EventDate> eventDate = accum.toEventDate();
    Assert.assertTrue(eventDate.isPresent());
    Assert.assertEquals(expected, eventDate.get());
  }
}
