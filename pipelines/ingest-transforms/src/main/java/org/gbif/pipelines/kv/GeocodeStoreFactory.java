package org.gbif.pipelines.kv;

import java.awt.image.BufferedImage;

import org.gbif.pipelines.parsers.config.model.KvConfig;
import org.gbif.pipelines.parsers.parsers.location.GeocodeBitmapCache;

import lombok.SneakyThrows;

/**
 * Factory to get singleton instance of {@link GeocodeStore}
 */
public class GeocodeStoreFactory {

  private final GeocodeBitmapCache cache;
  private static volatile GeocodeStoreFactory instance;
  private static final Object MUTEX = new Object();

  @SneakyThrows
  private GeocodeStoreFactory(KvConfig config, BufferedImage image) {
    cache = GeocodeStore.create(config, image);
  }

  public static GeocodeStoreFactory getInstance(KvConfig config, BufferedImage image) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GeocodeStoreFactory(config, image);
        }
      }
    }
    return instance;
  }

  public GeocodeBitmapCache getCache() {
    return cache;
  }

}
