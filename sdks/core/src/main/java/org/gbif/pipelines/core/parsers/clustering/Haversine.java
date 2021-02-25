package org.gbif.pipelines.core.parsers.clustering;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Haversine implementation providing approximate distance between 2 points. Credit to Jason Winn on
 * https://github.com/jasonwinn/haversine/blob/master/Haversine.java
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Haversine {
  private static final int APPROX_EARTH_RADIUS_KM = 6371;

  public static double distance(double startLat, double startLong, double endLat, double endLong) {

    double dLat = Math.toRadians((endLat - startLat));
    double dLong = Math.toRadians((endLong - startLong));

    startLat = Math.toRadians(startLat);
    endLat = Math.toRadians(endLat);

    double a = haversin(dLat) + Math.cos(startLat) * Math.cos(endLat) * haversin(dLong);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    return APPROX_EARTH_RADIUS_KM * c;
  }

  public static double haversin(double val) {
    return Math.pow(Math.sin(val / 2), 2);
  }
}
