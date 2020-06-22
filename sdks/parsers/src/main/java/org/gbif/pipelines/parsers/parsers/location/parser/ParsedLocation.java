package org.gbif.pipelines.parsers.parsers.location.parser;

import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.geocode.LatLng;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Models a parsed location. */
@AllArgsConstructor
@Getter
public class ParsedLocation {

  private final Country country;
  private final LatLng latLng;
}
