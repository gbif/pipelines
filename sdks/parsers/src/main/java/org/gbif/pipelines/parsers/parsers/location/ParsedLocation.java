package org.gbif.pipelines.parsers.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;

/** Models a parsed location. */
public class ParsedLocation {

  private final Country country;
  private final LatLng latLng;

  ParsedLocation(Country country, LatLng latLng) {
    this.country = country;
    this.latLng = latLng;
  }

  public Country getCountry() {
    return country;
  }

  public LatLng getLatLng() {
    return latLng;
  }
}
