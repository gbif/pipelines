package org.gbif.pipelines.core.parsers.location.parser;

import java.util.Objects;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.io.avro.GadmFeatures;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GadmParser {

  public static Optional<GadmFeatures> parseGadm(
      LocationRecord lr, KeyValueStore<LatLng, GeocodeResponse> kvStore) {
    Objects.requireNonNull(lr, "LocationRecord is required");
    Objects.requireNonNull(kvStore, "GeocodeService kvStore is required");

    // Take parsed values
    LatLng latLng = new LatLng(lr.getDecimalLatitude(), lr.getDecimalLongitude());

    // Use these to retrieve the GADM areas.
    // Check parameters
    Objects.requireNonNull(latLng);
    if (latLng.getLatitude() == null || latLng.getLongitude() == null) {
      throw new IllegalArgumentException("Empty coordinates");
    }

    // Match to GADM administrative regions
    return getGadmFromCoordinates(latLng, kvStore);
  }

  private static Optional<GadmFeatures> getGadmFromCoordinates(
      LatLng latLng, KeyValueStore<LatLng, GeocodeResponse> kvStore) {
    if (latLng.isValid()) {
      GeocodeResponse geocodeResponse = kvStore.get(latLng);

      if (geocodeResponse != null && !geocodeResponse.getLocations().isEmpty()) {
        GadmFeatures gf = GadmFeatures.newBuilder().build();
        geocodeResponse.getLocations().forEach(l -> accept(l, gf));
        return Optional.of(gf);
      }
    }
    return Optional.empty();
  }

  public static void accept(Location l, GadmFeatures gf) {
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
