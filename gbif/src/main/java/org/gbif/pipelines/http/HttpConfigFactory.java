package org.gbif.pipelines.http;

import org.gbif.pipelines.http.config.Config;
import org.gbif.pipelines.http.config.Service;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

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
    Objects.requireNonNull(service);
    Objects.requireNonNull(propertiesPath);

    return createConfigInternal(service, propertiesPath);
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
    config.setTimeout(Long.parseLong(props.getProperty(generatePropertyName(service, WS_TIMEOUT_PROP),
                                                       DEFAULT_TIMEOUT)));

    // cache properties
    String cacheName = service.name().toLowerCase().concat(DEFAULT_CACHE_NAME_SUFFIX);
    Long cacheSize =
      Long.parseLong(props.getProperty(generatePropertyName(service, CACHE_SIZE_PROP), DEFAULT_CACHE_SIZE))
      * 1024
      * 1024;

    Config.CacheConfig cacheConfig = new Config.CacheConfig();
    cacheConfig.setName(cacheName);
    cacheConfig.setSize(cacheSize);
    config.setCacheConfig(cacheConfig);

    return config;
  }

  private static Optional<Properties> loadProperties(Path propertiesPath) {
    Properties props = new Properties();
    try (InputStream in = Thread.currentThread()
      .getContextClassLoader()
      .getResourceAsStream(propertiesPath.toFile().getPath())) {
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
