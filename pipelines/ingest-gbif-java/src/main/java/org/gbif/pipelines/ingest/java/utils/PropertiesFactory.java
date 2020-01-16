package org.gbif.pipelines.ingest.java.utils;

import java.util.Properties;

import org.gbif.pipelines.ingest.utils.FsUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PropertiesFactory {

  private static Properties instance;

  public static synchronized Properties create(String hdfsSiteConfig, String propertiesPath) {
    if (instance == null) {
      instance = FsUtils.readPropertiesFile(hdfsSiteConfig, propertiesPath);

    }
    return instance;
  }

}
