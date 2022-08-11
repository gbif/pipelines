package org.gbif.pipelines.core.factory;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;

@Slf4j
@SuppressWarnings("all")
public class ConfigFactory<T> {

  private static volatile ConfigFactory instance;

  private final T config;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private ConfigFactory(HdfsConfigs hdfsConfigs, String propertiesPath, Class<T> clazz) {
    this.config = FsUtils.readConfigFile(hdfsConfigs, propertiesPath, clazz);
  }

  public static <T> ConfigFactory<T> getInstance(
      HdfsConfigs hdfsConfigs, String propertiesPath, Class<T> clazz) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new ConfigFactory(hdfsConfigs, propertiesPath, clazz);
        }
      }
    }
    return instance;
  }

  public T get() {
    return config;
  }
}
