package org.gbif.pipelines.core.interpretation;

public class Constant {

  private Constant() {
    // Can't have an instance
  }

  public static class Location {

    private Location() {
      // Can't have an instance
    }

    // COORDINATE_UNCERTAINTY_METERS bounds are exclusive bounds
    public static final double COORDINATE_UNCERTAINTY_METERS_LOWER_BOUND = 0;
    // 5000 km seems safe
    public static final double COORDINATE_UNCERTAINTY_METERS_UPPER_BOUND = 5000000;

    public static final double COORDINATE_PRECISION_LOWER_BOUND = 0;
    // 45 close to 5000 km
    public static final double COORDINATE_PRECISION_UPPER_BOUND = 45;
  }
}
