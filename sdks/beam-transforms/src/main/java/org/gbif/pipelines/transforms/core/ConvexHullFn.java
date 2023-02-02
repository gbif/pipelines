package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;
import lombok.Data;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.parsers.location.parser.ConvexHullParser;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.io.WKTWriter;

/** Beam that calculates a ConvexHull form all coordinates accumulated from location records. */
@Data
public class ConvexHullFn extends Combine.CombineFn<LocationRecord, ConvexHullFn.Accum, String> {

  private static final TupleTag<String> TAG = new TupleTag<String>() {};

  @Data
  public static class Accum implements Serializable {

    private Set<Coordinate> coordinates = new HashSet<>();

    public Accum acc(Set<Coordinate> coordinates) {
      this.coordinates.addAll(coordinates);
      return this;
    }

    public Accum acc(Coordinate coordinate) {
      coordinates.add(coordinate);
      return this;
    }

    public Optional<String> toWktConvexHull() {
      if (!coordinates.isEmpty()) {
        return Optional.of(
            new WKTWriter().write(ConvexHullParser.fromCoordinates(coordinates).getConvexHull()));
      }
      return Optional.empty();
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, LocationRecord input) {
    if (Optional.ofNullable(input.getHasCoordinate()).orElse(Boolean.FALSE)) {
      return mutableAccumulator.acc(
          new Coordinate(input.getDecimalLongitude(), input.getDecimalLatitude()));
    }
    return mutableAccumulator;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accumulators) {
    return StreamSupport.stream(accumulators.spliterator(), false)
        .reduce(
            new Accum(),
            (acc1, acc2) -> new Accum().acc(acc1.getCoordinates()).acc(acc2.getCoordinates()));
  }

  @Override
  public String extractOutput(Accum accumulator) {
    return accumulator.toWktConvexHull().orElse("");
  }

  public static TupleTag<String> tag() {
    return TAG;
  }
}
