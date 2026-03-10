package org.gbif.pipelines.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;

public class ConvexHullUtilsTest {

  @Test
  public void testEmptyCoordinates() {
    Optional<String> result = ConvexHullUtils.calculateGeometry(Set.of());
    assertFalse(result.isPresent());
  }

  @Test
  public void testSinglePoint() {
    Set<Coordinate> coords = Set.of(new Coordinate(10.0, 20.0));
    Optional<String> result = ConvexHullUtils.calculateGeometry(coords);
    assertTrue(result.isPresent());
    assertEquals("POINT(10.0 20.0)", result.get());
  }

  @Test
  public void testTwoPoints() {
    Set<Coordinate> coords = new LinkedHashSet<>();
    coords.add(new Coordinate(10.0, 20.0));
    coords.add(new Coordinate(30.0, 40.0));
    Optional<String> result = ConvexHullUtils.calculateGeometry(coords);
    assertTrue(result.isPresent());
    assertTrue(
        "Expected LINESTRING output, but got: " + result.get(),
        result.get().startsWith("LINESTRING"));
  }

  @Test
  public void testNormalPolygon() {
    // Four corners of a 10x10 square; area = 100 >> MIN_AREA (0.0001), no dateline crossing
    Set<Coordinate> coords = new LinkedHashSet<>();
    coords.add(new Coordinate(0.0, 0.0));
    coords.add(new Coordinate(10.0, 0.0));
    coords.add(new Coordinate(10.0, 10.0));
    coords.add(new Coordinate(0.0, 10.0));
    Optional<String> result = ConvexHullUtils.calculateGeometry(coords);
    assertTrue(result.isPresent());
    assertTrue(
        "Expected POLYGON output, but got: " + result.get(), result.get().startsWith("POLYGON"));
  }

  @Test
  public void testTinyTriangleFallsBackToEnvelope() {
    // Triangle with area = 0.5 * 0.005 * 0.005 = 0.0000125, which is below MIN_AREA (0.0001)
    // Expects the envelope (bounding box) to be returned as a POLYGON instead
    Set<Coordinate> coords = new LinkedHashSet<>();
    coords.add(new Coordinate(0.0, 0.0));
    coords.add(new Coordinate(0.005, 0.0));
    coords.add(new Coordinate(0.0, 0.005));
    Optional<String> result = ConvexHullUtils.calculateGeometry(coords);
    assertTrue(result.isPresent());
    assertTrue(
        "Expected POLYGON envelope fallback, but got: " + result.get(),
        result.get().startsWith("POLYGON"));
  }

  @Test
  public void testDatelineCrossing() {
    // Rectangle spanning from 170° to -170° longitude (width = 340° > 180°) triggers dateline
    // split, which should produce a MULTIPOLYGON covering both sides of the dateline.
    // splitPolygon computes realWest = max(-170, 170) = 170 and realEast = min(-170, 170) = -170,
    // then builds two boxes: [170..180] and [-180..-170].
    Set<Coordinate> coords = new LinkedHashSet<>();
    coords.add(new Coordinate(170.0, 0.0));
    coords.add(new Coordinate(170.0, 10.0));
    coords.add(new Coordinate(-170.0, 0.0));
    coords.add(new Coordinate(-170.0, 10.0));
    Optional<String> result = ConvexHullUtils.calculateGeometry(coords);
    assertTrue(result.isPresent());
    String expected =
        "MULTIPOLYGON(((170.0 0.0, 180.0 0.0, 180.0 10.0, 170.0 10.0, 170.0 0.0)),"
            + " ((-180.0 0.0, -170.0 0.0, -170.0 10.0, -180.0 10.0, -180.0 0.0)))";
    assertEquals(expected, result.get());
  }
}
