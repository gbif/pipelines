package org.gbif.pipelines.transforms.core;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.parsers.location.parser.ConvexHullParser;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.locationtech.jts.geom.Coordinate;
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
        Geometry geometry = ConvexHullParser.fromCoordinates(coordinates).getConvexHull();

        if (geometry.isValid()
            && !geometry.isEmpty()
            && geometry instanceof Polygon
            && geometry.getArea() > 0) {
          return Optional.of(new WKTWriter().write(geometry));
        }

        //        if (geometry.isValid() && !geometry.isEmpty()) {
        //          if (geometry.getArea() > 0) {
        //            String convexHull = new WKTWriter().write(geometry);
        //            log.info("Convex hull converted:{} ", convexHull);
        //            return Optional.of(convexHull);
        //          } else {
        //            // Get the bounding box envelope
        //            Geometry envelope = geometry.getEnvelope();
        //
        //            // Buffer slightly if envelope is still a line or point
        //            if (!(envelope instanceof Polygon)) {
        //              envelope = envelope.buffer(0.0001, 1);
        //            }
        //
        //            if (envelope instanceof Polygon && envelope.getArea() > 0) {
        //              String convexHull = new WKTWriter().write(envelope);
        //              log.info("Convex hull converted:{} ", convexHull);
        //              return Optional.of(convexHull);
        //            }
        //          }
        //        }
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
