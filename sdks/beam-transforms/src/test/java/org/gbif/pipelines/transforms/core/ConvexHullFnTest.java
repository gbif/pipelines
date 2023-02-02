package org.gbif.pipelines.transforms.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

public class ConvexHullFnTest {

  @Test
  @SneakyThrows
  public void accumulatorTest() {
    WKTReader wktReader = new WKTReader();

    Geometry geometry =
        wktReader.read(
            "POLYGON((100000 515000,100000 520000,105000 520000,105000 515000,100000 515000))");

    // Tests an accumulator built from a collection of coordinates
    ConvexHullFn.Accum accum = new ConvexHullFn.Accum();
    List<Coordinate> coordinates = Arrays.asList(geometry.getCoordinates());
    accum.acc(new HashSet<>(coordinates));
    assertAccum(accum, geometry);

    // Tests an accumulator built accumulating single coordinates
    ConvexHullFn.Accum accum2 = new ConvexHullFn.Accum();
    coordinates.forEach(accum2::acc);
    assertAccum(accum2, geometry);
  }

  /** Test the that an accumulator produces the same convex hull as the one produced by JTS. */
  private void assertAccum(ConvexHullFn.Accum accum, Geometry geometry) {
    WKTWriter wktWriter = new WKTWriter();
    Optional<String> convexHull = accum.toWktConvexHull();
    Assert.assertTrue(convexHull.isPresent());
    Assert.assertEquals(wktWriter.write(geometry.convexHull()), convexHull.get());
  }
}
