package org.gbif.pipelines.kv;

import java.awt.image.BufferedImage;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

import org.gbif.kvs.geocode.LatLng;
import org.gbif.pipelines.parsers.config.model.KvConfig;
import org.gbif.pipelines.parsers.parsers.location.cache.GeocodeBitmapCache;
import org.gbif.rest.client.geocode.GeocodeResponse;

import javax.imageio.ImageIO;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GeocodeBitmapCacheFactory {

  private static volatile GeocodeBitmapCacheFactory instance;

  private final GeocodeBitmapCache cache;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private GeocodeBitmapCacheFactory(KvConfig config, Function<LatLng, GeocodeResponse> loadFn) {
    this.cache = GeocodeBitmapCache.create(loadImageFile(config.getImagePath()), loadFn);
  }

  public static GeocodeBitmapCache getInstance(KvConfig config, Function<LatLng, GeocodeResponse> loadFn) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new GeocodeBitmapCacheFactory(config, loadFn);
        }
      }
    }
    return instance.cache;
  }

  @SneakyThrows
  private BufferedImage loadImageFile(String filePath) {
    Path path = Paths.get(filePath);
    if (!path.isAbsolute()) {
      try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath)) {
        if (is == null) {
          throw new FileNotFoundException("Can't load image from resource - " + filePath);
        }
        return ImageIO.read(is);
      }
    }
    throw new FileNotFoundException("The image file doesn't exist - " + filePath);
  }
}
