package org.gbif.pipelines.parsers.parsers.location;

import org.gbif.common.parsers.geospatial.LatLng;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.gbif.api.vocabulary.OccurrenceIssue.PRESUMED_NEGATED_LATITUDE;
import static org.gbif.api.vocabulary.OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE;
import static org.gbif.api.vocabulary.OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE;

/** Models a function that can be applied to a coordinates. */
class CoordinatesFunction {

  private CoordinatesFunction() {}

  static final Function<LatLng, LatLng> NEGATED_LAT_FN =
      latLng -> new LatLng(-1d * latLng.getLat(), latLng.getLng());
  static final Function<LatLng, LatLng> NEGATED_LNG_FN =
      latLng -> new LatLng(latLng.getLat(), -1d * latLng.getLng());
  static final Function<LatLng, LatLng> NEGATED_COORDS_FN =
      latLng -> new LatLng(-1d * latLng.getLat(), -1d * latLng.getLng());
  static final Function<LatLng, LatLng> SWAPPED_COORDS_FN =
      latLng -> new LatLng(latLng.getLng(), latLng.getLat());

  static List<String> getIssueTypes(Function<LatLng, LatLng> transformation) {
    if (transformation == NEGATED_LAT_FN) {
      return Collections.singletonList(PRESUMED_NEGATED_LATITUDE.name());
    }
    if (transformation == NEGATED_LNG_FN) {
      return Collections.singletonList(PRESUMED_NEGATED_LONGITUDE.name());
    }
    if (transformation == NEGATED_COORDS_FN) {
      return Arrays.asList(PRESUMED_NEGATED_LATITUDE.name(), PRESUMED_NEGATED_LONGITUDE.name());
    }
    if (transformation == SWAPPED_COORDS_FN) {
      return Collections.singletonList(PRESUMED_SWAPPED_COORDINATE.name());
    }

    return Collections.emptyList();
  }
}
