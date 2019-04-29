package org.gbif.pipelines.parsers.config;

import java.nio.file.Path;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Creates the configuration to use a specific WS.
 *
 * <p>By default it reads the configuration from the "http.properties" file.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class WsConfigFactory {

  public static WsConfig create(@NonNull Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props = ConfigFactory.loadProperties(propertiesPath);

    // get the base path or throw exception if not present
    return org.aeonbits.owner.ConfigFactory.create(WsConfig.class, props);
  }
}
