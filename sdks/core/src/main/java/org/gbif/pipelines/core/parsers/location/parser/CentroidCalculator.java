package org.gbif.pipelines.core.parsers.location.parser;

import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CentroidCalculator {

  public static Optional<Double> calculateCentroidDistance(
      LocationRecord lr, KeyValueStore<LatLng, GeocodeResponse> kvStore) {
    Objects.requireNonNull(lr, "LocationRecord is required");
    Objects.requireNonNull(kvStore, "GeocodeService kvStore is required");

    // Take parsed values. Uncertainty isn't needed, but included anyway so we hit the cache.
    LatLng latLng =
        LatLng.create(
            lr.getDecimalLatitude(),
            lr.getDecimalLongitude(),
            lr.getCoordinateUncertaintyInMeters());
    // Use these to retrieve the centroid distances.
    // Check parameters
    Objects.requireNonNull(latLng);
    if (latLng.getLatitude() == null || latLng.getLongitude() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }

    return getDistanceToNearestCentroid(latLng, kvStore);
  }

  private static Optional<Double> getDistanceToNearestCentroid(
      LatLng latLng, KeyValueStore<LatLng, GeocodeResponse> geocodeKvStore) {
    if (latLng.isValid()) {
      GeocodeResponse geocodeResponse = geocodeKvStore.get(latLng);
      if (geocodeResponse != null && !geocodeResponse.getLocations().isEmpty()) {
        Optional<Double> centroidDistance =
            geocodeResponse.getLocations().stream()
                .filter(l -> "Centroids".equals(l.getType()))
                .sorted(Comparator.comparingDouble(Location::getDistanceMeters))
                .findFirst()
                .map(Location::getDistanceMeters);
        return centroidDistance;
      }
    }
    return Optional.empty();
  }
}
