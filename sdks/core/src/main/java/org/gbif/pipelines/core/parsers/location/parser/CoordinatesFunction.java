package org.gbif.pipelines.core.parsers.location.parser;

import static org.gbif.api.vocabulary.OccurrenceIssue.PRESUMED_NEGATED_LATITUDE;
import static org.gbif.api.vocabulary.OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE;
import static org.gbif.api.vocabulary.OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.geocode.GeocodeRequest;

/** Models a function that can be applied to a coordinates. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CoordinatesFunction {

  public static final UnaryOperator<GeocodeRequest> NEGATED_LAT_FN =
      latLng ->
          GeocodeRequest.create(
              -1d * latLng.getLat(), latLng.getLng(), latLng.getUncertaintyMeters());
  public static final UnaryOperator<GeocodeRequest> NEGATED_LNG_FN =
      latLng ->
          GeocodeRequest.create(
              latLng.getLat(), -1d * latLng.getLng(), latLng.getUncertaintyMeters());
  public static final UnaryOperator<GeocodeRequest> NEGATED_COORDS_FN =
      latLng ->
          GeocodeRequest.create(
              -1d * latLng.getLat(), -1d * latLng.getLng(), latLng.getUncertaintyMeters());
  public static final UnaryOperator<GeocodeRequest> SWAPPED_COORDS_FN =
      latLng ->
          GeocodeRequest.create(latLng.getLng(), latLng.getLat(), latLng.getUncertaintyMeters());

  public static Set<String> getIssueTypes(UnaryOperator<GeocodeRequest> transformation) {
    if (transformation == NEGATED_LAT_FN) {
      return Collections.singleton(PRESUMED_NEGATED_LATITUDE.name());
    }
    if (transformation == NEGATED_LNG_FN) {
      return Collections.singleton(PRESUMED_NEGATED_LONGITUDE.name());
    }
    if (transformation == NEGATED_COORDS_FN) {
      return new TreeSet<>(
          Arrays.asList(PRESUMED_NEGATED_LATITUDE.name(), PRESUMED_NEGATED_LONGITUDE.name()));
    }
    if (transformation == SWAPPED_COORDS_FN) {
      return Collections.singleton(PRESUMED_SWAPPED_COORDINATE.name());
    }

    return Collections.emptySet();
  }
}
