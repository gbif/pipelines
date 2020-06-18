package org.gbif.pipelines.ingest.java.utils;

import org.gbif.pipelines.ingest.utils.FsUtils;
import org.gbif.pipelines.parsers.config.model.PipelinesConfig;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PipelinesConfigFactory {

  private static volatile PipelinesConfigFactory instance;

  private final PipelinesConfig config;

  private static final Object MUTEX = new Object();

  @SneakyThrows
  private PipelinesConfigFactory(String hdfsSiteConfig, String propertiesPath) {
    this.config = FsUtils.readConfigFile(hdfsSiteConfig, propertiesPath);
  }

  public static PipelinesConfigFactory getInstance(String hdfsSiteConfig, String propertiesPath) {
    if (instance == null) {
      synchronized (MUTEX) {
        if (instance == null) {
          instance = new PipelinesConfigFactory(hdfsSiteConfig, propertiesPath);
        }
      }
    }
    return instance;
  }

  public PipelinesConfig get() {
    return config;
  }
}
