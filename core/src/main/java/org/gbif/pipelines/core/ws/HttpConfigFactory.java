package org.gbif.pipelines.core.ws;

import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.Service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the configuration to use a specific WS. The supported web services are defined in {@link Service}.
 * <p>
 * By default it reads the configurarion from the "http.properties" file.
 */
public class HttpConfigFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HttpConfigFactory.class);

  // default properties
  private static final String PROPERTIES_FILE_PATH_DEFAULT = "ws.properties";

  // property suffixes
  private static final String WS_BASE_PATH_PROP = ".http.basePath";
  private static final String WS_TIMEOUT_PROP = ".http.timeoutSeconds";
  private static final String CACHE_SIZE_PROP = ".cache.sizeInMb";

  // defaults
  private static final String DEFAULT_TIMEOUT = "60";
  private static final String DEFAULT_CACHE_SIZE = "256";

  private static final String DEFAULT_CACHE_NAME_SUFFIX = "-cacheWs";

  private HttpConfigFactory() {}

  public static Config createConfig(Service service) {
    Objects.requireNonNull(service);

    // using default properties
    Path propertiesPath = Paths.get(PROPERTIES_FILE_PATH_DEFAULT);

    return createConfigInternal(service, propertiesPath);
  }

  public static Config createConfig(Service service, Path propertiesPath) {
    return createConfigInternal(Objects.requireNonNull(service), Objects.requireNonNull(propertiesPath));
  }

  private static Config createConfigInternal(Service service, Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props =
      loadProperties(propertiesPath).orElseThrow(() -> new IllegalArgumentException("Could not load properties file "
                                                                                    + propertiesPath));

    // get the base path or throw exception if not present
    String basePath = Optional.ofNullable(props.getProperty(generatePropertyName(service, WS_BASE_PATH_PROP)))
      .filter(prop -> !prop.isEmpty())
      .orElseThrow(() -> new IllegalArgumentException("WS base path is required"));

    // set config properties
    Config config = new Config();
    config.setBasePath(basePath);
    config.setTimeout(Long.parseLong(props.getProperty(generatePropertyName(service, WS_TIMEOUT_PROP), DEFAULT_TIMEOUT)));

    // cache properties
    String cacheName = service.name().toLowerCase().concat(DEFAULT_CACHE_NAME_SUFFIX);

    long configSize = Long.parseLong(props.getProperty(generatePropertyName(service, CACHE_SIZE_PROP), DEFAULT_CACHE_SIZE));
    Long cacheSize = configSize * 1024L * 1024L; // Cache in megabytes

    Config.CacheConfig cacheConfig = new Config.CacheConfig();
    cacheConfig.setName(cacheName);
    cacheConfig.setSize(cacheSize);
    config.setCacheConfig(cacheConfig);

    return config;
  }

  private static Optional<Properties> loadProperties(Path propertiesPath) {
    Function<Path, InputStream> absolute = path -> {
      try {
        return new FileInputStream(path.toFile());
      } catch (FileNotFoundException ex) {
        LOG.error("Properties could not be read from {}", propertiesPath.toString(), ex);
        throw new IllegalArgumentException(ex.getMessage(), ex);
      }
    };

    Function<Path, InputStream> resource =
      path -> Thread.currentThread().getContextClassLoader().getResourceAsStream(path.toString());

    Function<Path, InputStream> function = propertiesPath.isAbsolute() ? absolute : resource;

    Properties props = new Properties();
    try (InputStream in = function.apply(propertiesPath)) {
      // read properties from input stream
      props.load(in);
    } catch (Exception e) {
      LOG.error("Properties could not be read from {}", propertiesPath.toString(), e);
      return Optional.empty();
    }

    return Optional.of(props);
  }

  private static String generatePropertyName(Service service, String property) {
    return service.getPath() + property;
  }
}
