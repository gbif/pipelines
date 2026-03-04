package org.gbif.pipelines.core.parsers.location.parser;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

/**
 * Utility to transform elements into <a href="https://en.wikipedia.org/wiki/Convex_hull">Convex
 * Hull</a>.
 */
@UtilityClass
public class ConvexHullParser {

  public static final double PRECISION = 1_000_000;

  /** Concatenates two arrays. */
  private static <T> T[] concat(T[] array1, T[] array2) {
    T[] both = Arrays.copyOf(array1, array1.length + array2.length);
    System.arraycopy(array2, 0, both, array1.length, array2.length);
    return both;
  }

  public org.locationtech.jts.algorithm.ConvexHull fromCoordinates(Coordinate[] coordinates) {
    return new FromCoordinates().apply(Arrays.asList(coordinates));
  }

  public org.locationtech.jts.algorithm.ConvexHull fromCoordinates(
      Collection<Coordinate> coordinates) {
    return new FromCoordinates().apply(coordinates);
  }

  public Optional<org.locationtech.jts.algorithm.ConvexHull> fromConvexHulls(
      Collection<org.locationtech.jts.algorithm.ConvexHull> convexHulls) {
    return new FromConvexHulls().apply(convexHulls);
  }

  /**
   * Transforms a collection of object into a <a
   * href="https://en.wikipedia.org/wiki/Convex_hull">Convex Hull</a>.
   */
  public static class FromObject<T>
      implements BiFunction<
          Collection<T>, Function<T, Coordinate>, org.locationtech.jts.algorithm.ConvexHull> {

    @Override
    public org.locationtech.jts.algorithm.ConvexHull apply(
        Collection<T> collection, Function<T, Coordinate> mapper) {
      List<Coordinate> coordinates = collection.stream().map(mapper).collect(Collectors.toList());
      return new FromCoordinates().apply(coordinates);
    }
  }

  /**
   * Transforms a collection of coordinates into a <a
   * href="https://en.wikipedia.org/wiki/Convex_hull">Convex Hull</a>.
   */
  public static class FromCoordinates
      implements Function<Collection<Coordinate>, org.locationtech.jts.algorithm.ConvexHull> {

    @Override
    public org.locationtech.jts.algorithm.ConvexHull apply(Collection<Coordinate> coordinates) {
      return new org.locationtech.jts.algorithm.ConvexHull(
          coordinates.toArray(new Coordinate[] {}), new GeometryFactory());
    }
  }

  /**
   * Transforms a collection of convex hulls into a <a
   * href="https://en.wikipedia.org/wiki/Convex_hull">Convex Hull</a>.
   */
  public static class FromConvexHulls
      implements Function<
          Collection<org.locationtech.jts.algorithm.ConvexHull>,
          Optional<org.locationtech.jts.algorithm.ConvexHull>> {

    @Override
    public Optional<org.locationtech.jts.algorithm.ConvexHull> apply(
        Collection<org.locationtech.jts.algorithm.ConvexHull> convexHulls) {
      return convexHulls.stream()
          .reduce(
              (ch1, ch2) ->
                  new org.locationtech.jts.algorithm.ConvexHull(
                      concat(
                          ch1.getConvexHull().getCoordinates(),
                          ch2.getConvexHull().getCoordinates()),
                      new GeometryFactory(new PrecisionModel(PRECISION))));
    }
  }
}
