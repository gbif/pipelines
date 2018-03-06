package org.gbif.pipelines.labs.interpretation.lineage.quality;

public class LocationAssessments {

  private LocationAssessments() {
    //do nothing
  }

  public static Assertion<Integer> maxAltitudeCheck() {
    return Assertion.validation((Integer altitude) -> altitude <= 10_000, "Altitude can't be greater than 10000");
  }

  public static Assertion<Integer> maxDepthCheck() {
    return Assertion.validation((Integer depth) -> depth <= 0, "Depth can't be less than 10000");
  }
}
