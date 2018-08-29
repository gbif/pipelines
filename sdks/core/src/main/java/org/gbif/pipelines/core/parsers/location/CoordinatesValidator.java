package org.gbif.pipelines.core.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;

public final class CoordinatesValidator {

  // Antarctica: "Territories south of 60° south latitude"
  private static final double ANTARCTICA_LATITUDE = -60;

  private CoordinatesValidator() {}

  public static boolean isInRange(LatLng latLng) {
    return Double.compare(latLng.getLat(), 90) <= 0
           && Double.compare(latLng.getLat(), -90) >= 0
           && Double.compare(latLng.getLng(), 180) <= 0
           && Double.compare(latLng.getLng(), -180) >= 0;
  }

  static void checkEmptyCoordinates(LatLng latLng) {
    if (latLng.getLat() == null || latLng.getLng() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }
  }

  /**
   * Checks if the country and latitude belongs to Antarctica. Rule: country must be
   * Country.ANTARCTICA or null and latitude must be less than (south of) {@link
   * #ANTARCTICA_LATITUDE} but not less than -90°.
   */
  static boolean isAntarctica(Double latitude, Country country) {
    return latitude != null
        && (country == null || country == Country.ANTARCTICA)
        && (latitude >= -90 && latitude < ANTARCTICA_LATITUDE);
  }
}
