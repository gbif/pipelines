package org.gbif.pipelines.ingest.java.utils;

import org.gbif.pipelines.ingest.utils.FsUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("all")
public class ConfigFactory<T> {

  private static volatile ConfigFactory instance;

  private final T config;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private ConfigFactory(String hdfsSiteConfig, String propertiesPath, Class<T> clazz) {
    this.config = FsUtils.readConfigFile(hdfsSiteConfig, propertiesPath, clazz);
  }

  public static <T> ConfigFactory<T> getInstance(String hdfsSiteConfig, String propertiesPath, Class<T> clazz) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ConfigFactory(hdfsSiteConfig, propertiesPath, clazz);
        }
      }
    }
    return instance;
  }

  public T get() {
    return config;
  }
}
