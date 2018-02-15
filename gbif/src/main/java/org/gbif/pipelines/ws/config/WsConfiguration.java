package org.gbif.pipelines.ws.config;

import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the configuration to use a specific WS. The supported web services are defined in {@link Service}.
 * <p>
 * By default it reads the configurarion from the "ws.properties" file.
 */
public class WsConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(WsConfiguration.class);

  // default properties
  private static final String PROPERTIES_FILE_PATH_DEFAULT = "ws.properties";

  // property suffixes
  private static final String WS_BASE_PATH_PROP_SUFFIX = ".ws.basePath";
  private static final String WS_TIMEOUT_PROP_SUFFIX = ".ws.timeoutSeconds";
  private static final String CACHE_NAME_PROP_SUFFIX = ".cache.name";
  private static final String CACHE_SIZE_PROP_SUFFIX = ".cache.sizeInMb";

  // defaults
  private static final String DEFAULT_TIMEOUT = "60";
  private static final String DEFAULT_CACHE_SIZE = "100";

  private String propertiesPath;
  private final Service service;
  private boolean cacheDisabled;
  private Config config;

  // error
  private String errorMessage;

  private WsConfiguration(Service service) {
    this.service = service;
  }

  public WsConfiguration fromProperties(String propertiesPath) {
    this.propertiesPath = propertiesPath;
    return this;
  }

  public static WsConfiguration of(Service service) {
    return new WsConfiguration(service);
  }

  public WsConfiguration withCacheDisabled() {
    cacheDisabled = true;
    return this;
  }

  public Optional<Config> getConfig() {
    createConfig();
    return Optional.ofNullable(config);
  }

  public Config getConfigOrThrowException() {
    createConfig();
    if (Strings.isNullOrEmpty(errorMessage) && config != null) {
      return config;
    }
    throw new IllegalArgumentException(errorMessage);
  }

  // FIXME: should this function be received as a parameter??
  private void createConfig() {
    Objects.requireNonNull(service);

    Optional<Properties> optionalProps = loadProperties();

    // check if properties could be loaded
    if (!optionalProps.isPresent()) {
      errorMessage = "Could not load properties file " + propertiesPath;
      return;
    }

    Properties props = optionalProps.get();

    Optional<String> basePath =
      Optional.ofNullable(props.getProperty(propertyName(WS_BASE_PATH_PROP_SUFFIX))).filter(path -> !path.isEmpty());

    // check if the base path is set in the properties file
    if (!basePath.isPresent()) {
      errorMessage = "WS base path is required";
      return;
    }

    Optional<String> cacheName =
      Optional.ofNullable(props.getProperty(propertyName(CACHE_NAME_PROP_SUFFIX))).filter(prop -> !prop.isEmpty());

    // check if the cache config is set in the properties file
    if (!cacheName.isPresent() && !cacheDisabled) {
      errorMessage = "cache properties are required";
      return;
    }

    // set config properties
    config = new Config();
    config.setBasePath(basePath.get());
    config.setTimeout(Long.parseLong(props.getProperty(propertyName(WS_TIMEOUT_PROP_SUFFIX), DEFAULT_TIMEOUT)));

    if (cacheName.isPresent()) {
      // set cache properties
      Config.CacheConfig cacheConfig = new Config.CacheConfig();
      cacheConfig.setName(cacheName.get());
      cacheConfig.setSize(Long.parseLong(props.getProperty(propertyName(CACHE_SIZE_PROP_SUFFIX), DEFAULT_CACHE_SIZE))
                          * 1024
                          * 1024);

      config.setCacheConfig(cacheConfig);
    }
  }

  private Optional<Properties> loadProperties() {
    String path = Optional.ofNullable(propertiesPath).orElse(PROPERTIES_FILE_PATH_DEFAULT);

    Properties props = new Properties();
    try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
      props.load(in);
    } catch (Exception e) {
      LOG.error("Could not load properties file {}", path, e);
      return Optional.empty();
    }

    return Optional.of(props);
  }

  private String propertyName(String propSuffix) {
    return service.getPath() + propSuffix;
  }
}
