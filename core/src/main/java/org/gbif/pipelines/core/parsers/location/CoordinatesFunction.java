package org.gbif.pipelines.core.parsers.location;

import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.pipelines.io.avro.issue.IssueType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Models a function that can be applied to a coordinates.
 */
public enum CoordinatesFunction {

  IDENTITY(Function.identity()), // no location transform
  PRESUMED_NEGATED_LAT(Transformers.NEGATED_LAT_FN), // lat negated
  PRESUMED_NEGATED_LNG(Transformers.NEGATED_LNG_FN), // lng negated
  PRESUMED_NEGATED_COORDS(Transformers.NEGATED_COORDS_FN), // both coords negated
  PRESUMED_SWAPPED_COORDS(Transformers.SWAPPED_COORDS_FN); // coords swapped

  private final Function<LatLng, LatLng> transformer;

  CoordinatesFunction(Function<LatLng, LatLng> transformer) {
    this.transformer = transformer;
  }

  public Function<LatLng, LatLng> getTransformer() {
    return transformer;
  }

  private static class Transformers {

    private static final Function<LatLng, LatLng> NEGATED_LAT_FN =
      latLng -> new LatLng(-1 * latLng.getLat(), latLng.getLng());
    private static final Function<LatLng, LatLng> NEGATED_LNG_FN =
      latLng -> new LatLng(latLng.getLat(), -1 * latLng.getLng());
    private static final Function<LatLng, LatLng> NEGATED_COORDS_FN =
      latLng -> new LatLng(-1 * latLng.getLat(), -1 * latLng.getLng());
    private static final Function<LatLng, LatLng> SWAPPED_COORDS_FN =
      latLng -> new LatLng(latLng.getLng(), latLng.getLat());
  }

  public static List<IssueType> getIssueTypes(CoordinatesFunction transformation) {
    if (transformation == PRESUMED_NEGATED_LAT) {
      return Collections.singletonList(IssueType.PRESUMED_NEGATED_LATITUDE);
    }
    if (transformation == PRESUMED_NEGATED_LNG) {
      return Collections.singletonList(IssueType.PRESUMED_NEGATED_LONGITUDE);
    }
    if (transformation == PRESUMED_NEGATED_COORDS) {
      return Arrays.asList(IssueType.PRESUMED_NEGATED_LATITUDE, IssueType.PRESUMED_NEGATED_LONGITUDE);
    }
    if (transformation == PRESUMED_SWAPPED_COORDS) {
      return Collections.singletonList(IssueType.PRESUMED_SWAPPED_COORDINATE);
    }

    return Collections.emptyList();
  }

}
