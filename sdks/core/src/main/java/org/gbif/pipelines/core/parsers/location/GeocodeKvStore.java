package org.gbif.pipelines.core.parsers.location;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.Serializable;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.parsers.location.cache.GeocodeBitmapCache;
import org.gbif.rest.client.geocode.GeocodeResponse;

@Slf4j
public class GeocodeKvStore implements KeyValueStore<LatLng, GeocodeResponse>, Serializable {

  private static final long serialVersionUID = -2090636199984570712L;

  private final KeyValueStore<LatLng, GeocodeResponse> kvStore;
  private final GeocodeBitmapCache bitmapCache;

  private GeocodeKvStore(
      @NonNull KeyValueStore<LatLng, GeocodeResponse> kvStore,
      BufferedImage image,
      String kvStoreType,
      boolean missEqualsFail) {
    this.kvStore = kvStore;
    if (image != null) {
      this.bitmapCache =
          GeocodeBitmapCache.create(image, kvStore::get, kvStoreType, missEqualsFail);
    } else {
      this.bitmapCache = null;
      log.info("Image cache path is empty, skipping bitmapCache initialisation");
    }
  }

  public static GeocodeKvStore create(
      KeyValueStore<LatLng, GeocodeResponse> kvStore, BufferedImage image) {
    return new GeocodeKvStore(kvStore, image, GeocodeBitmapCache.DEFAULT_KV_STORE, true);
  }

  public static GeocodeKvStore create(KeyValueStore<LatLng, GeocodeResponse> kvStore) {
    return new GeocodeKvStore(kvStore, null, GeocodeBitmapCache.DEFAULT_KV_STORE, true);
  }

  public static GeocodeKvStore create(
      KeyValueStore<LatLng, GeocodeResponse> kvStore,
      BufferedImage image,
      String kvStoreType,
      boolean missEqualsFail) {
    return new GeocodeKvStore(kvStore, image, kvStoreType, missEqualsFail);
  }

  public static GeocodeKvStore create(
      KeyValueStore<LatLng, GeocodeResponse> kvStore, String kvStoreType, boolean missEqualsFail) {
    return new GeocodeKvStore(kvStore, null, kvStoreType, missEqualsFail);
  }

  /** Simple get candidates by point. */
  @Override
  public GeocodeResponse get(LatLng latLng) {
    GeocodeResponse locations = null;

    // Check the image map for a sure location.
    if (bitmapCache != null) {
      locations = bitmapCache.getFromBitmap(latLng);
    }

    // If that doesn't help, use the database.
    if (locations == null
        || locations.getLocations() == null
        || locations.getLocations().isEmpty()) {
      locations = kvStore.get(latLng);
    }

    return locations;
  }

  @Override
  public void close() {
    if (kvStore != null) {
      try {
        kvStore.close();
      } catch (IOException ex) {
        log.error("Error closing KVStore", ex);
      }
    }
  }
}
