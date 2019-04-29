package org.gbif.pipelines.parsers.config;

import java.nio.file.Path;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KvConfigFactory {

  public static KvConfig create(@NonNull Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);

    // get the base path or throw exception if not present
    return org.aeonbits.owner.ConfigFactory.create(KvConfig.class, props);
  }

}
