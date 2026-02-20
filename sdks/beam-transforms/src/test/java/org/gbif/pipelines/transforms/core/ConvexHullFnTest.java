package org.gbif.pipelines.transforms.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
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

    Geometry geometry = wktReader.read("POLYGON((10 51.5,10 52,10.5 52,10.5 51.5,10 51.5))");

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

  @Test
  public void emptyAreaTest() {
    double[][] coords = {
      {47.00369, 33.58338},
      {47.01489, 33.656816},
      {47.11748, 33.56188},
      {46.9915, 33.650652},
      {47.097354, 33.563785},
      {47.003866, 33.582036},
      {47.050661, 33.578531},
      {46.999675, 33.654026},
      {47.08014, 33.723998},
      {47.080232, 33.723874},
      {47.117563, 33.562479},
      {46.99248, 33.64777},
      {47.024126, 33.655432},
      {47.07545, 33.71094},
      {47.028423, 33.65366},
      {47.010316, 33.607002},
      {47.023848, 33.654977},
      {47.023784, 33.654851},
      {47.02224, 33.65627},
      {47.028536, 33.653571},
      {47.047595, 33.58167},
      {47.04762, 33.58149},
      {47.09756, 33.56389},
      {47.080186, 33.723936},
      {47.050621, 33.578461},
      {47.00152, 33.655198},
      {47.023816, 33.654914}
    };

    ConvexHullFn.Accum accum = new ConvexHullFn.Accum();
    Function<Double, Double> round = v -> Math.round(v * 100000.0) / 100000.0;
    Arrays.stream(coords)
        .forEach(
            c -> {
              accum.acc(new Coordinate(round.apply(c[1]), round.apply(c[0])));
            });

    Assert.assertEquals(
        "POLYGON ((33.56188 47.11748, 33.56248 47.11756, 33.72387 47.08023, 33.72394 47.08019, 33.724 47.08014,"
            + " 33.65065 46.9915, 33.58204 47.00387, 33.56379 47.09735, 33.56188 47.11748))",
        accum.toWktConvexHull().orElse(""));
  }

  @Test
  public void crossesDatelineTest() {
    double[][] coords = {
      {51.512, 176.057}, {60.486, 179.428}, {54.28, 168.744}, {58.364, 169.723},
      {51.39, 171.271}, {51.515, 176.109}, {52.263, 172.974}, {52.286, 172.274},
      {52.263, 172.974}, {57.54, 164.358}, {59.675, 170.727}, {55.258, 167.733},
      {55.384, 167.269}, {60.331, 179.567}, {58.364, 169.723}, {51.515, 176.109},
      {51.688, 176.755}, {60.331, 179.567}, {60.053, 171.31}, {55.398, 167.27},
      {53.408, 171.173}, {51.684, 176.794}, {54.377, 167.065}, {54.619, 165.872},
      {55.823, 165.445}, {57.044, 165.676}, {57.074, 164.044}, {58.364, 169.723},
      {60.053, 171.31}, {55.398, 167.27}, {55.384, 167.269}, {55.398, 167.27},
      {55.823, 165.445}, {58.364, 169.723}, {51.688, 176.755}, {51.684, 176.794},
      {60.053, 171.31}, {58.364, 169.723}, {51.343, -177.128}, {57.54, 164.358},
      {59.675, 170.727}, {60.053, 171.31}, {55.398, 167.238}, {51.515, 176.109},
      {59.675, 170.727}, {59.675, 170.727}, {55.823, 165.445}, {59.675, 170.727},
      {59.675, 170.727}, {57.044, 165.676}, {55.636, 165.013}, {55.823, 165.445},
      {55.398, 167.238}, {59.675, 170.727}, {54.377, 167.065}, {55.523, 164.854},
      {60.053, 171.31}, {56.671, 166.104}, {55.398, 167.27}, {57.074, 164.044},
      {60.486, 179.428}, {51.69, 176.782}, {54.28, 168.744}, {55.398, 167.27},
      {58.785, 169.997}, {51.688, 176.755}, {55.384, 167.269}, {57.074, 164.044},
      {54.431, 167.148}, {55.398, 167.238}, {55.384, 167.269}, {55.398, 167.27},
      {60.331, 179.567}, {55.398, 167.27}, {51.688, 176.755}, {57.084, 164.324},
      {51.343, -177.128}, {55.398, 167.238}, {60.331, 179.567}
    };

    ConvexHullFn.Accum accum = new ConvexHullFn.Accum();
    Arrays.stream(coords)
        .forEach(
            c -> {
              accum.acc(new Coordinate(c[1], c[0]));
            });

    Assert.assertEquals(
        "MULTIPOLYGON(((179.567 51.343, 180.0 51.343, 180.0 60.486, 179.567 60.486, 179.567 51.343)), "
            + "((-180.0 51.343, -177.128 51.343, -177.128 60.486, -180.0 60.486, -180.0 51.343)))",
        accum.toWktConvexHull().orElse(""));
  }
}
