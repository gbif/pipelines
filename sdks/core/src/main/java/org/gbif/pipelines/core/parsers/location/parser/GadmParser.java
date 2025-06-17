package org.gbif.pipelines.core.parsers.location.parser;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.GeocodeRequest;
import org.gbif.pipelines.core.interpreters.model.GadmFeatures;
import org.gbif.pipelines.core.interpreters.model.LocationRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GadmParser {

  public static Optional<GadmFeatures> parseGadm(
          LocationRecord lr,
          KeyValueStore<GeocodeRequest, GeocodeResponse> kvStore,
          Function<Void, GadmFeatures> createFn
          ) {
    Objects.requireNonNull(lr, "LocationRecord is required");
    Objects.requireNonNull(kvStore, "GeocodeService kvStore is required");

    // Take parsed values. Uncertainty isn't needed, but included anyway so we hit the cache.
    GeocodeRequest latLng =
        GeocodeRequest.create(
            lr.getDecimalLatitude(),
            lr.getDecimalLongitude(),
            lr.getCoordinateUncertaintyInMeters());

    // Use these to retrieve the GADM areas.
    // Check parameters
    Objects.requireNonNull(latLng);
    if (latLng.getLat() == null || latLng.getLng() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }

    // Match to GADM administrative regions
    return getGadmFromCoordinates(latLng, kvStore, createFn);
  }

  private static Optional<GadmFeatures> getGadmFromCoordinates(
      GeocodeRequest latLng,
      KeyValueStore<GeocodeRequest, GeocodeResponse> kvStore,
      Function<Void, GadmFeatures> createFn) {

    if (latLng.isValid()) {
      GeocodeResponse geocodeResponse = kvStore.get(latLng);

      if (geocodeResponse != null && !geocodeResponse.getLocations().isEmpty()) {
        GadmFeatures gf = createFn.apply(null);
        geocodeResponse.getLocations().forEach(l -> accept(l, gf));
        return Optional.of(gf);
      }
    }
    return Optional.empty();
  }

  public static void accept(GeocodeResponse.Location l, GadmFeatures gf) {
    if (l.getType() != null && l.getDistance() != null && l.getDistance() == 0) {
      switch (l.getType()) {
        case "GADM0":
          if (gf.getLevel0Gid() == null) {
            gf.setLevel0Gid(l.getId());
            gf.setLevel0Name(l.getName());
          }
          return;
        case "GADM1":
          if (gf.getLevel1Gid() == null) {
            gf.setLevel1Gid(l.getId());
            gf.setLevel1Name(l.getName());
          }
          return;
        case "GADM2":
          if (gf.getLevel2Gid() == null) {
            gf.setLevel2Gid(l.getId());
            gf.setLevel2Name(l.getName());
          }
          return;
        case "GADM3":
          if (gf.getLevel3Gid() == null) {
            gf.setLevel3Gid(l.getId());
            gf.setLevel3Name(l.getName());
          }
          return;
        default:
      }
    }
  }
}
