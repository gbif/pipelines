package org.gbif.pipelines.ingest.java.utils;

import java.util.Properties;

import org.gbif.pipelines.ingest.utils.FsUtils;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PropertiesFactory {

  private static volatile PropertiesFactory instance;

  private final Properties properties;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private PropertiesFactory(String hdfsSiteConfig, String propertiesPath) {
    this.properties = FsUtils.readPropertiesFile(hdfsSiteConfig, propertiesPath);
  }

  public static PropertiesFactory getInstance(String hdfsSiteConfig, String propertiesPath) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new PropertiesFactory(hdfsSiteConfig, propertiesPath);
        }
      }
    }
    return instance;
  }

  public Properties get() {
    return properties;
  }

}
