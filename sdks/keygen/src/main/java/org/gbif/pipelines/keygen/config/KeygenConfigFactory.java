package org.gbif.pipelines.keygen.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KeygenConfigFactory {

  private static final String PREFIX = "keygen.table.";

  public static final String HBASE_ZK = "zookeeper.url";
  public static final String OCC_TABLE = PREFIX + "occ";
  public static final String COUNTER_TABLE = PREFIX + "counter";
  public static final String LOOKUP_TABLE = PREFIX + "lookup";

  public static KeygenConfig create(@NonNull Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props = loadProperties(propertiesPath);

    return create(props);
  }

  public static KeygenConfig create(@NonNull Properties props) {
    // get the base path or throw exception if not present
    UnaryOperator<String> fn = key ->
        Optional.ofNullable(props.getProperty(key))
            .filter(prop -> !prop.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException(key + " - can't find the value!"));

    String hbaseZk = fn.apply(HBASE_ZK);
    String occTable = fn.apply(OCC_TABLE);
    String counterTable = fn.apply(COUNTER_TABLE);
    String lookupTable = fn.apply(LOOKUP_TABLE);

    return KeygenConfig.create(occTable, counterTable, lookupTable, hbaseZk);
  }

  /**
   *
   */
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
