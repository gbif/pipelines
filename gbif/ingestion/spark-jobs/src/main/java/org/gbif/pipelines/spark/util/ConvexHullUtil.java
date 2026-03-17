package org.gbif.pipelines.spark.util;

import java.util.Optional;
import java.util.Set;
import org.gbif.pipelines.core.parsers.location.parser.ConvexHullParser;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.WKTWriter;

public class ConvexHullUtil {

  private static final double MIN_AREA = 0.0001;
  private static final double PRECISION = 1_000_000;

  public static Optional<String> calculateGeometry(Set<Coordinate> coordinates) {
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

  private static boolean crossesDateline(Geometry geometry) {
    return geometry.getEnvelopeInternal().getWidth() > 180.0;
  }

  private static String splitPolygon(Envelope env) {
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

  private static void appendBox(StringBuilder sb, double x1, double x2, double y1, double y2) {
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
