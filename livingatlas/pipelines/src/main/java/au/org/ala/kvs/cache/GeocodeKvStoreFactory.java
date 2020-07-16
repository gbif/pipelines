package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import java.awt.image.BufferedImage;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.factory.BufferedImageFactory;
import org.gbif.pipelines.parsers.parsers.location.GeocodeKvStore;
import org.gbif.pipelines.transforms.SerializableSupplier;
import org.gbif.rest.client.geocode.GeocodeResponse;

/** Factory to get singleton instance of {@link GeocodeKvStore} */
public class GeocodeKvStoreFactory {

  private static final Object MUTEX = new Object();
  private static volatile GeocodeKvStoreFactory instance;
  private final KeyValueStore<LatLng, GeocodeResponse> countryKvStore;
  private final KeyValueStore<LatLng, GeocodeResponse> stateProvinceKvStore;

  @SneakyThrows
  private GeocodeKvStoreFactory(ALAPipelinesConfig config) {
    BufferedImage image =
        BufferedImageFactory.getInstance(config.getGbifConfig().getImageCachePath());
    KeyValueStore<LatLng, GeocodeResponse> countryStore =
        CountryKeyValueStore.create(config.getGeocodeConfig());
    KeyValueStore<LatLng, GeocodeResponse> stateProvinceStore =
        StateProvinceKeyValueStore.create(config.getGeocodeConfig());
    countryKvStore = GeocodeKvStore.create(countryStore, image);
    stateProvinceKvStore = GeocodeKvStore.create(stateProvinceStore);
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
