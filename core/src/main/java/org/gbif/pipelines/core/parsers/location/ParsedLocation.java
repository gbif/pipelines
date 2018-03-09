package org.gbif.pipelines.core.parsers.location;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;

/**
 * Models a parsed location.
 */
public class ParsedLocation {

  private final Country country;
  private final LatLng latLng;

  private ParsedLocation(Builder builder) {
    this.country = builder.country;
    this.latLng = builder.latLng;
  }

  public Country getCountry() {
    return country;
  }

  public LatLng getLatLng() {
    return latLng;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Country country;
    private LatLng latLng;

    Builder country(Country country) {
      this.country = country;
      return this;
    }

    Builder latLng(LatLng latLng) {
      this.latLng = latLng;
      return this;
    }

    ParsedLocation build() {
      return new ParsedLocation(this);
    }

  }

}
