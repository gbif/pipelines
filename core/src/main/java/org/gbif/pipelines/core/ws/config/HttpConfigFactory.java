package org.gbif.pipelines.core.ws.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the configuration to use a specific WS. The supported web services are defined in {@link
 * Service}.
 *
 * <p>By default it reads the configurarion from the "http.properties" file.
 */
public class HttpConfigFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HttpConfigFactory.class);

  // property suffixes
  private static final String WS_BASE_PATH_PROP = ".http.basePath";
  private static final String WS_TIMEOUT_PROP = ".http.timeoutSeconds";
  private static final String CACHE_SIZE_PROP = ".cache.sizeInMb";

  // property defaults
  private static final String DEFAULT_TIMEOUT_PROP = "60";
  private static final String DEFAULT_CACHE_SIZE_IN_MB_PROP = "256";

  // long defaults
  static final long DEFAULT_TIMEOUT = Long.parseLong(DEFAULT_TIMEOUT_PROP);
  static final long DEFAULT_CACHE_SIZE =
      Long.parseLong(DEFAULT_CACHE_SIZE_IN_MB_PROP) * 1024L * 1024L;

  private HttpConfigFactory() {}

  public static Config createConfig(Service service, Path propertiesPath) {
    return createConfigInternal(
        Objects.requireNonNull(service), Objects.requireNonNull(propertiesPath));
  }

  /** Creates a {@link Config} from a url and uses default timeout and cache size. */
  public static Config createConfigFromUrl(String url) {
    return createConfigFromUrl(url, DEFAULT_TIMEOUT, DEFAULT_CACHE_SIZE);
  }

  /** Creates a {@link Config} from a url and uses default timeout and cache size. */
  public static Config createConfigFromUrl(String url, long timeout, long cacheSize) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "url is required");
    return new Config.Builder().basePath(url).timeout(timeout).cacheSize(cacheSize).build();
  }

  private static Config createConfigInternal(Service service, Path propertiesPath) {
    // load properties or throw exception if cannot be loaded
    Properties props =
        loadProperties(propertiesPath)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Could not load properties file " + propertiesPath));
    // get the base path or throw exception if not present
    String basePath =
        Optional.ofNullable(props.getProperty(generatePropertyName(service, WS_BASE_PATH_PROP)))
            .filter(prop -> !prop.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("WS base path is required"));

    // set config properties
    Config.Builder builder = new Config.Builder();
    builder.basePath(basePath);
    builder.timeout(
        Long.parseLong(
            props.getProperty(
                generatePropertyName(service, WS_TIMEOUT_PROP), DEFAULT_TIMEOUT_PROP)));

    long configSize =
        Long.parseLong(
            props.getProperty(
                generatePropertyName(service, CACHE_SIZE_PROP), DEFAULT_CACHE_SIZE_IN_MB_PROP));
    long cacheSize = configSize * 1024L * 1024L; // Cache in megabytes
    builder.cacheSize(cacheSize);

    return builder.build();
  }

  private static Optional<Properties> loadProperties(Path propertiesPath) {
    Function<Path, InputStream> absolute =
        path -> {
          try {
            return new FileInputStream(path.toFile());
          } catch (FileNotFoundException ex) {
            LOG.error(
                "Properties with absolute path could not be read from {}",
                propertiesPath.toString(),
                ex);
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
      LOG.error("Properties could not be load from {}", propertiesPath.toString(), e);
      return Optional.empty();
    }

    return Optional.of(props);
  }

  private static String generatePropertyName(Service service, String property) {
    return service.getPath() + property;
  }
}
