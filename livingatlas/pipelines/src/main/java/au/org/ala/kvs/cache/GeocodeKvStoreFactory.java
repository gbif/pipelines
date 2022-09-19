package au.org.ala.kvs.cache;

import au.org.ala.kvs.ALAPipelinesConfig;
import java.awt.image.BufferedImage;
import java.util.Collections;
import lombok.SneakyThrows;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.core.functions.SerializableSupplier;
import org.gbif.pipelines.core.parsers.location.GeocodeKvStore;
import org.gbif.pipelines.core.parsers.location.cache.BinaryBitmapLookup;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
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
    marine.setName("MARINE");
    BIOME_MARINE = new GeocodeResponse(Collections.singletonList(marine));
  }

  @SneakyThrows
  private GeocodeKvStoreFactory(ALAPipelinesConfig config) {

    HdfsConfigs hdfsConfigs = HdfsConfigs.nullConfig();

    BufferedImage image =
        BufferedImageFactory.getInstance(hdfsConfigs, config.getGbifConfig().getImageCachePath());
    KeyValueStore<LatLng, GeocodeResponse> countryStore =
        CountryKeyValueStore.create(config.getGeocodeConfig());

    // missEqualsFail=true because each point should be associated with a country or marine area
    this.countryKvStore = GeocodeKvStore.create(countryStore, image, "COUNTRY", true);

    KeyValueStore<LatLng, GeocodeResponse> stateProvinceStore =
        StateProvinceKeyValueStore.create(config.getGeocodeConfig());

    // Try to load from image file which has the same name of the SHP file
    BufferedImage stateCacheImage =
        BufferedImageFactory.loadImageFile(
            hdfsConfigs, config.getGeocodeConfig().getStateProvince().getPath() + BITMAP_EXT);

    // missEqualsFail=false because not every point will be in a stateProvince
    this.stateProvinceKvStore =
        GeocodeKvStore.create(stateProvinceStore, stateCacheImage, "STATEPROVINCE", false);

    // Try to load from image file which has the same name of the SHP file
    BufferedImage biomeCacheImage =
        BufferedImageFactory.loadImageFile(
            hdfsConfigs, config.getGeocodeConfig().getBiome().getPath() + BITMAP_EXT);

    this.biomeKvStore =
        new KeyValueStore<LatLng, GeocodeResponse>() {

          final BinaryBitmapLookup bbl = BinaryBitmapLookup.create(biomeCacheImage, "BIOME");

          @Override
          public void close() {
            // NOP
          }

          @Override
          public GeocodeResponse get(LatLng latLng) {
            boolean isTerrestrial = bbl.intersects(latLng);
            if (isTerrestrial) {
              return BIOME_TERRESTRIAL;
            }
            return BIOME_MARINE;
          }
        };
  }

  private static GeocodeKvStoreFactory getInstance(ALAPipelinesConfig config) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GeocodeKvStoreFactory(config);
        }
      }
    }
    return instance;
  }

  public static SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> createCountrySupplier(
      ALAPipelinesConfig config) {
    return () -> getInstance(config).countryKvStore;
  }

  public static SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>>
      createStateProvinceSupplier(ALAPipelinesConfig config) {
    return () -> getInstance(config).stateProvinceKvStore;
  }

  public static SerializableSupplier<KeyValueStore<LatLng, GeocodeResponse>> createBiomeSupplier(
      ALAPipelinesConfig config) {
    return () -> getInstance(config).biomeKvStore;
  }
}
