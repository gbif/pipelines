package org.gbif.pipelines.transforms.core;

import static org.gbif.pipelines.core.parsers.location.parser.ConvexHullParser.PRECISION;

import java.io.Serializable;
import java.util.HashSet;
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
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.WKTWriter;

/** Beam that calculates a ConvexHull form all coordinates accumulated from location records. */
@Data
@Slf4j
public class ConvexHullFn extends Combine.CombineFn<LocationRecord, ConvexHullFn.Accum, String> {

  private static final TupleTag<String> TAG = new TupleTag<String>() {};

  @Data
  public static class Accum implements Serializable {

    private static final double MIN_AREA = 0.0001;
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
      if (coordinates.isEmpty()) {
        return Optional.empty();
      }

      if (coordinates.size() == 1) {
        Coordinate coord = coordinates.iterator().next();
        return Optional.of("POINT(" + coord.x + " " + coord.y + ")");
      }

      if (coordinates.size() == 2) {
        LineString lineString =
            new LineString(
                new CoordinateArraySequence(coordinates.toArray(new Coordinate[] {})),
                new GeometryFactory(new PrecisionModel(PRECISION)));
        return Optional.of(new WKTWriter().write(lineString));
      }

      Geometry geometry = ConvexHullParser.fromCoordinates(coordinates).getConvexHull();
      geometry.normalize();

      if (!geometry.isValid() || geometry.isEmpty()) {
        return Optional.empty();
      }

      if (geometry instanceof Polygon
          && geometry.getArea() >= MIN_AREA
          && !crossesDateline(geometry)) {
        return Optional.of(new WKTWriter().write(geometry));
      }

      if (crossesDateline(geometry)) {
        return Optional.of(splitPolygon(geometry.getEnvelopeInternal()));
      }

      if (geometry.getArea() < MIN_AREA) {
        // Get the bounding box envelope
        Geometry envelope = geometry.getEnvelope();

        // Buffer slightly if envelope is still a line or point
        if (!(envelope instanceof Polygon)) {
          envelope = envelope.buffer(MIN_AREA, 1);
        }

        if (envelope instanceof Polygon && envelope.getArea() > 0) {
          return Optional.of(new WKTWriter().write(envelope));
        }
      }

      return Optional.empty();
    }

    private boolean crossesDateline(Geometry geometry) {
      return geometry.getEnvelopeInternal().getWidth() > 180.0;
    }

    private String splitPolygon(Envelope env) {
      double westStart = env.getMinX();
      double eastEnd = env.getMaxX();
      double south = env.getMinY();
      double north = env.getMaxY();

      // take into account negative longitudes
      double realWest = Math.max(westStart, eastEnd);
      double realEast = Math.min(westStart, eastEnd);

      StringBuilder sb = new StringBuilder("MULTIPOLYGON(");

      // from realWest to 180
      sb.append("((");
      appendBox(sb, realWest, 180.0, south, north);
      sb.append(")), ");

      // from -180 to realEast
      sb.append("((");
      appendBox(sb, -180.0, realEast, south, north);
      sb.append("))");

      sb.append(")");
      return sb.toString();
    }

    private void appendBox(StringBuilder sb, double x1, double x2, double y1, double y2) {
      appendCoord(sb, x1, y1).append(", ");
      appendCoord(sb, x2, y1).append(", ");
      appendCoord(sb, x2, y2).append(", ");
      appendCoord(sb, x1, y2).append(", ");
      appendCoord(sb, x1, y1);
    }

    private static StringBuilder appendCoord(StringBuilder sb, double x, double y) {
      return sb.append(x).append(" ").append(y);
    }
  }

  @Override
  public Accum createAccumulator() {
    return new Accum();
  }

  @Override
  public Accum addInput(Accum mutableAccumulator, LocationRecord input) {
    if (Optional.ofNullable(input.getHasCoordinate()).orElse(Boolean.FALSE)) {

      Function<Double, Double> round = v -> Math.round(v * PRECISION) / PRECISION;

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
