package org.gbif.pipelines.core.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.gbif.pipelines.core.parsers.location.parser.ConvexHullParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Stopwatch;
import org.junit.runner.Description;
import org.locationtech.jts.algorithm.ConvexHull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvexHullParserTest {

  private static final Logger LOG = LoggerFactory.getLogger(ConvexHullParserTest.class);

  private static void logInfo(Description description, String status, long nanos) {
    String testName = description.getMethodName();
    LOG.info(
        String.format(
            "Test %s %s, spent %d milliseconds",
            testName, status, TimeUnit.NANOSECONDS.toMillis(nanos)));
  }

  @Rule
  public Stopwatch stopwatch =
      new Stopwatch() {
        @Override
        protected void succeeded(long nanos, Description description) {
          logInfo(description, "succeeded", nanos);
        }
      };

  @SneakyThrows
  @Test
  public void conversionTest() {

    WKTReader reader = new WKTReader();

    // Uses Canada since it was the largest polygon found in PostGIS
    // select ST_AsText(geom) from political order by char_length(ST_AsText(geom)) desc limit 1
    Geometry canada =
        reader.read(
            new InputStreamReader(
                ConvexHullParserTest.class
                    .getClassLoader()
                    .getResourceAsStream("wkt-test/canada.wkt")));

    ConvexHull convexHull = ConvexHullParser.fromCoordinates(canada.getCoordinates());
    Geometry convexHullGeom = convexHull.getConvexHull();

    // Are both convex hulls identical
    assertEquals(canada.convexHull(), convexHullGeom);

    // There was a reduction in the amount of vertices
    assertTrue(canada.getCoordinates().length > convexHullGeom.getCoordinates().length);

    // The convex hull contains the original geometry
    assertTrue(convexHullGeom.contains(canada));
  }
}
