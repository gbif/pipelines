package org.gbif.pipelines.keygen.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Function;

import org.aeonbits.owner.ConfigFactory;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KeygenConfigFactory {

  public static KeygenConfig create(@NonNull Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props = loadProperties(propertiesPath);

    // get the base path or throw exception if not present
    return ConfigFactory.create(KeygenConfig.class, props);
  }

  private static Properties loadProperties(Path propertiesPath) {
    Function<Path, InputStream> absolute = path -> {
      try {
        return new FileInputStream(path.toFile());
      } catch (Exception ex) {
        String msg = "Properties with absolute path could not be read from " + propertiesPath;
        throw new IllegalArgumentException(msg, ex);
      }
    };

    Function<Path, InputStream> resource =
        path -> Thread.currentThread().getContextClassLoader().getResourceAsStream(path.toString());

    Function<Path, InputStream> function = propertiesPath.isAbsolute() ? absolute : resource;

    Properties props = new Properties();
    try (InputStream in = function.apply(propertiesPath)) {
      // read properties from input stream
      props.load(in);
    } catch (Exception ex) {
      String msg = "Properties with absolute path could not be read from " + propertiesPath;
      throw new IllegalArgumentException(msg, ex);
    }

    return props;
  }

}
