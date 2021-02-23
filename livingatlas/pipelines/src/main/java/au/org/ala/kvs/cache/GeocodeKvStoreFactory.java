package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Collections;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.pipelines.factory.BufferedImageFactory;
import org.gbif.rest.client.geocode.GeocodeResponse;
import org.gbif.rest.client.geocode.Location;

/** Factory to get singleton instance of {@link GeocodeKvStore} */
public class GeocodeKvStoreFactory {

  private final KeyValueStore<LatLng, GeocodeResponse> countryKvStore;
  private final KeyValueStore<LatLng, GeocodeResponse> stateProvinceKvStore;
  private final KeyValueStore<LatLng, GeocodeResponse> biomeKvStore;
  private static volatile GeocodeKvStoreFactory instance;
  private static final Object MUTEX = new Object();
  private static final String BITMAP_EXT = ".png";

  static final GeocodeResponse BIOME_TERRESTRIAL;
  static final GeocodeResponse BIOME_MARINE;

  static {
    final Location terrestrial = new Location();
    terrestrial.setName("TERRESTRIAL");
    BIOME_TERRESTRIAL = new GeocodeResponse(Collections.singletonList(terrestrial));
    final Location marine = new Location();
    terrestrial.setName("MARINE");
    BIOME_MARINE = new GeocodeResponse(Collections.singletonList(terrestrial));
  }

  @SneakyThrows
  private GeocodeKvStoreFactory(ALAPipelinesConfig config) {
    BufferedImage image =
        BufferedImageFactory.getInstance(config.getGbifConfig().getImageCachePath());
    KeyValueStore<LatLng, GeocodeResponse> countryStore =
        CountryKeyValueStore.create(config.getGeocodeConfig());

    // missEqualsFail=true because each point should be associated with a country or marine area
    this.countryKvStore = GeocodeKvStore.create(countryStore, image, "COUNTRY", true);

    KeyValueStore<LatLng, GeocodeResponse> stateProvinceStore =
        StateProvinceKeyValueStore.create(config.getGeocodeConfig());

    // Try to load from image file which has the same name of the SHP file
    BufferedImage stateCacheImage =
        BufferedImageFactory.loadImageFile(
            config.getGeocodeConfig().getStateProvince().getPath() + BITMAP_EXT);

    // missEqualsFail=false because not every point will be in a stateProvince
    this.stateProvinceKvStore =
        GeocodeKvStore.create(stateProvinceStore, stateCacheImage, "STATEPROVINCE", false);

    // Try to load from image file which has the same name of the SHP file
    BufferedImage biomeCacheImage =
        BufferedImageFactory.loadImageFile(
            config.getGeocodeConfig().getBiome().getPath() + BITMAP_EXT);

    // uses the GADM layer
    KeyValueStore<LatLng, GeocodeResponse> biomeStoreLoadFn =
        new KeyValueStore<LatLng, GeocodeResponse>() {
          @Override
          public void close() throws IOException {
            // NOP
          }

          @Override
          public GeocodeResponse get(LatLng latLng) {
            return BIOME_TERRESTRIAL;
          }
        };

    GeocodeKvStore gadmBitMapStore =
        GeocodeKvStore.create(biomeStoreLoadFn, biomeCacheImage, "BIOME", false);

    this.biomeKvStore =
        new KeyValueStore<LatLng, GeocodeResponse>() {
          @Override
          public void close() throws IOException {
            // NOP
          }

          @Override
          public GeocodeResponse get(LatLng latLng) {
            GeocodeResponse response = gadmBitMapStore.get(latLng);
            if (response != null
                && response.getLocations() != null
                && !response.getLocations().isEmpty()) {
              return response;
            }
            return BIOME_MARINE;
          }
        };
  }

  public static KeyValueStore<LatLng, GeocodeResponse> getInstance(ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GeocodeKvStoreFactory(config);
        }
      }
    }
    return instance.countryKvStore;
  }

  public static SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> createCountrySupplier(
      ALAPipelinesConfig config) {
    return () -> new GeocodeKvStoreFactory(config).countryKvStore;
  }

  public static SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>>
      createStateProvinceSupplier(ALAPipelinesConfig config) {
    return () -> new GeocodeKvStoreFactory(config).stateProvinceKvStore;
  }

  public static SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> createBiomeSupplier(
      ALAPipelinesConfig config) {
    return () -> new GeocodeKvStoreFactory(config).biomeKvStore;
  }
}
