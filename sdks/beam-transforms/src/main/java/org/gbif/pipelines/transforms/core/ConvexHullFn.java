package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.parsers.location.parser.ConvexHullParser;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKTWriter;

/** Beam that calculates a ConvexHull form all coordinates accumulated from location records. */
@Data
@Slf4j
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
        if (coordinates.size() == 1) {
          Coordinate coord = coordinates.iterator().next();
          return Optional.of("POINT(" + coord.x + " " + coord.y + ")");
        }

        if (coordinates.size() == 2) {
          StringBuilder wktBuilder = new StringBuilder("LINESTRING(");

          Iterator<Coordinate> iterator = coordinates.iterator();
          while (iterator.hasNext()) {
            Coordinate coord = iterator.next();
            double lon = coord.x;
            wktBuilder.append(lon).append(" ").append(coord.y);

            if (iterator.hasNext()) {
              wktBuilder.append(", ");
            }
          }

          wktBuilder.append(")");
          return Optional.of(wktBuilder.toString());
        }

        Geometry geometry = ConvexHullParser.fromCoordinates(coordinates).getConvexHull();
        geometry.normalize();

        if (!geometry.isValid() && geometry.isEmpty()) {
          return Optional.empty();
        }

        if (geometry instanceof Polygon && geometry.getArea() > 0 && !crossesDateline(geometry)) {
          return Optional.of(new WKTWriter().write(geometry));
        }

        if (crossesDateline(geometry)) {
          Envelope env = geometry.getEnvelopeInternal();
          return Optional.of(
              String.format(
                  "ENVELOPE(%f, %f, %f, %f)",
                  env.getMinX(), env.getMaxX(), env.getMaxY(), env.getMinY()));
        }

        if (geometry.getArea() == 0) {
          // Get the bounding box envelope
          Geometry envelope = geometry.getEnvelope();

          // Buffer slightly if envelope is still a line or point
          if (!(envelope instanceof Polygon)) {
            envelope = envelope.buffer(0.0001, 1);
          }

          if (envelope instanceof Polygon && envelope.getArea() > 0) {
            return Optional.of(new WKTWriter().write(envelope));
          }
        }
      }
      return Optional.empty();
    }

    private boolean crossesDateline(Geometry geometry) {
      return geometry.getEnvelopeInternal().getWidth() > 180.0;
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, LocationRecord input) {
    if (Optional.ofNullable(input.getHasCoordinate()).orElse(Boolean.FALSE)) {

      Function<Double, Double> round = v -> Math.round(v * 1000000.0) / 1000000.0;

      return mutableAccumulator.acc(
          new Coordinate(
              round.apply(input.getDecimalLongitude()), round.apply(input.getDecimalLatitude())));
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
