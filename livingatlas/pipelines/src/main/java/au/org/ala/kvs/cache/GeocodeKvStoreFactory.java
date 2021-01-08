package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import java.awt.image.BufferedImage;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.pipelines.factory.BufferedImageFactory;
import org.gbif.rest.client.geocode.GeocodeResponse;

/** Factory to get singleton instance of {@link GeocodeKvStore} */
public class GeocodeKvStoreFactory {

  private final KeyValueStore<LatLng, GeocodeResponse> countryKvStore;
  private final KeyValueStore<LatLng, GeocodeResponse> stateProvinceKvStore;
  private static volatile GeocodeKvStoreFactory instance;
  private static final Object MUTEX = new Object();
  private static final String BITMAP_EXT = ".png";

  @SneakyThrows
  private GeocodeKvStoreFactory(ALAPipelinesConfig config) {
    BufferedImage image =
        BufferedImageFactory.getInstance(config.getGbifConfig().getImageCachePath());
    KeyValueStore<LatLng, GeocodeResponse> countryStore =
        CountryKeyValueStore.create(config.getGeocodeConfig());

    // missEqualsFail=true because each point should be associated with a country or marine area
    countryKvStore = GeocodeKvStore.create(countryStore, image, "COUNTRY", true);

    KeyValueStore<LatLng, GeocodeResponse> stateProvinceStore =
        StateProvinceKeyValueStore.create(config.getGeocodeConfig());

    // Try to load from image file which has the same name of the SHP file
    BufferedImage stateCacheImage =
        BufferedImageFactory.loadImageFile(
            config.getGeocodeConfig().getStateProvince().getPath() + BITMAP_EXT);

    // missEqualsFail=false because not every point will be in a stateProvince
    stateProvinceKvStore =
        GeocodeKvStore.create(stateProvinceStore, stateCacheImage, "STATEPROVINCE", false);
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
}
