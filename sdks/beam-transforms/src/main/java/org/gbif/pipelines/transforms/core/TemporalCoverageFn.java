package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;
import lombok.Data;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.TemporalRecord;

/** Beam combine function that extracts the max and min dates from EventDate records. */
@Data
public class TemporalCoverageFn
    extends Combine.CombineFn<TemporalRecord, TemporalCoverageFn.Accum, EventDate> {

  private static final TupleTag<EventDate> TAG = new TupleTag<EventDate>() {};

  @Data
  public static class Accum implements Serializable {

    private String minDate;
    private String maxDate;

    public Accum acc(EventDate eventDate) {
      Optional.ofNullable(eventDate.getGte()).ifPresent(this::setMinDate);
      Optional.ofNullable(eventDate.getLte()).ifPresent(this::setMaxDate);
      return this;
    }

    private void setMinDate(String date) {
      if (Objects.isNull(minDate)) {
        minDate = date;
      } else {
        minDate =
            StringToDateFunctions.getStringToDateFn()
                        .apply(date)
                        .compareTo(StringToDateFunctions.getStringToDateFn().apply(minDate))
                    < 0
                ? date
                : minDate;
      }
    }

    private void setMaxDate(String date) {
      if (Objects.isNull(maxDate)) {
        maxDate = date;
      } else {
        maxDate =
            StringToDateFunctions.getStringToDateFn()
                        .apply(date)
                        .compareTo(StringToDateFunctions.getStringToDateFn().apply(maxDate))
                    > 0
                ? date
                : maxDate;
      }
    }

    public Optional<EventDate> toEventDate() {
      return Objects.isNull(minDate) && Objects.isNull(maxDate)
          ? Optional.empty()
          : Optional.of(getEventDate());
    }

    private EventDate getEventDate() {
      EventDate.Builder evenDate = EventDate.newBuilder();
      Optional.ofNullable(minDate).ifPresent(evenDate::setGte);
      Optional.ofNullable(maxDate).ifPresent(evenDate::setLte);
      return evenDate.build();
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, TemporalRecord input) {
    Optional.ofNullable(input.getEventDate()).ifPresent(mutableAccumulator::acc);
    return mutableAccumulator;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accumulators) {
    return StreamSupport.stream(accumulators.spliterator(), false)
        .reduce(
            new Accum(),
            (acc1, acc2) -> new Accum().acc(acc1.getEventDate()).acc(acc2.getEventDate()));
  }

  @Override
  public EventDate extractOutput(Accum accumulator) {
    return accumulator.toEventDate().orElse(EventDate.newBuilder().build());
  }

  public static TupleTag<EventDate> tag() {
    return TAG;
  }
}
