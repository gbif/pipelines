package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;
import lombok.Data;
import org.apache.beam.sdk.transforms.Combine;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;

@Data
public class TemporalCoverageFn
    extends Combine.CombineFn<TemporalRecord, TemporalCoverageFn.Accum, DerivedMetadataRecord> {

  private final DerivedMetadataRecord derivedMetadataRecord;

  @Data
  public static class Accum implements Serializable {

    private String minDate;
    private String maxDate;

    public Accum acc(EventDate eventDate) {
      Optional.ofNullable(eventDate.getLte()).ifPresent(this::setMinDate);

      Optional.ofNullable(eventDate.getGte()).ifPresent(this::setMaxDate);

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
      Optional.ofNullable(minDate).ifPresent(evenDate::setLte);
      Optional.ofNullable(maxDate).ifPresent(evenDate::setGte);
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
  public DerivedMetadataRecord extractOutput(Accum accumulator) {
    accumulator
        .toEventDate()
        .map(
            jEd ->
                org.gbif.pipelines.io.avro.json.EventDate.newBuilder()
                    .setLte(jEd.getLte())
                    .setGte(jEd.getGte())
                    .build())
        .ifPresent(derivedMetadataRecord::setTemporalCoverage);
    return derivedMetadataRecord;
  }
}
