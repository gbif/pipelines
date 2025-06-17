package org.gbif.pipelines.core.parsers.location.parser;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.interpreters.model.LocationRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CentroidCalculator {

  // The maximum distance to set, beyond this the value will be left null.
  // https://github.com/gbif/portal-feedback/issues/4232#issuecomment-1306884884
  public static final double MAXIMUM_DISTANCE_FROM_CENTROID_METRES = 5000;

  public static Optional<Double> calculateCentroidDistance(
          LocationRecord lr, KeyValueStore<GeocodeRequest, GeocodeResponse> kvStore) {
    Objects.requireNonNull(lr, "LocationRecord is required");
    Objects.requireNonNull(kvStore, "GeocodeService kvStore is required");

    // Take parsed values. Uncertainty isn't needed, but included anyway so we hit the cache.
    GeocodeRequest latLng =
        GeocodeRequest.create(
            lr.getDecimalLatitude(),
            lr.getDecimalLongitude(),
            lr.getCoordinateUncertaintyInMeters());
    // Use these to retrieve the centroid distances.
    // Check parameters
    Objects.requireNonNull(latLng);
    if (latLng.getLat() == null || latLng.getLng() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }

    return getDistanceToNearestCentroid(latLng, kvStore);
  }

  private static Optional<Double> getDistanceToNearestCentroid(
      GeocodeRequest latLng, KeyValueStore<GeocodeRequest, GeocodeResponse> geocodeKvStore) {
    if (latLng.isValid()) {
      GeocodeResponse geocodeResponse = geocodeKvStore.get(latLng);
      if (geocodeResponse != null && !geocodeResponse.getLocations().isEmpty()) {
        Optional<Double> centroidDistance =
            geocodeResponse.getLocations().stream()
                .filter(
                    l ->
                        "Centroids".equals(l.getType())
                            && l.getDistanceMeters() != null
                            && l.getDistanceMeters() <= MAXIMUM_DISTANCE_FROM_CENTROID_METRES)
                .sorted(Comparator.comparingDouble(GeocodeResponse.Location::getDistanceMeters))
                .findFirst()
                .map(GeocodeResponse.Location::getDistanceMeters);
        return centroidDistance;
      }
    }
    return Optional.empty();
  }
}
