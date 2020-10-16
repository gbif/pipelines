package org.gbif.pipelines.core.parsers.location.parser;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.gbif.api.vocabulary.Country;
import org.gbif.kvs.geocode.LatLng;

/** Models a parsed location. */
@AllArgsConstructor
@Getter
public class ParsedLocation {

  private final Country country;
  private final LatLng latLng;
}
