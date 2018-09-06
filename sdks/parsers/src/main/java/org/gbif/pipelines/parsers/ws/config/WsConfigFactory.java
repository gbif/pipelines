package org.gbif.pipelines.parsers.ws.config;

import org.gbif.pipelines.parsers.exception.IORuntimeException;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Creates the configuration to use a specific WS.
 *
 * <p>By default it reads the configurarion from the "http.properties" file.
 */
public class WsConfigFactory {

  // property suffixes
  private static final String WS_BASE_PATH_PROP = "gbif.api.url";
  private static final String WS_TIMEOUT_PROP = ".timeout";
  private static final String CACHE_SIZE_PROP = ".cache.size";

  // property defaults
  private static final String DEFAULT_TIMEOUT = "60";
  private static final String DEFAULT_CACHE_SIZE_MB = "64";

  private WsConfigFactory() {}

  public static WsConfig create(String wsName, Path propertiesPath) {
    Objects.requireNonNull(wsName);
    Objects.requireNonNull(propertiesPath);
    // load properties or throw exception if cannot be loaded
    Properties props = loadProperties(propertiesPath);

    // get the base path or throw exception if not present
    String basePath =
        Optional.ofNullable(props.getProperty(WS_BASE_PATH_PROP))
            .filter(prop -> !prop.isEmpty())
            .orElseThrow(() -> new IllegalArgumentException("WS base path is required"));

    String cacheSize = props.getProperty(wsName + CACHE_SIZE_PROP, DEFAULT_CACHE_SIZE_MB);
    String timeout = props.getProperty(wsName + WS_TIMEOUT_PROP, DEFAULT_TIMEOUT);

    return new WsConfig(basePath, timeout, cacheSize);
  }

  /** Creates a {@link WsConfig} from a url and uses default timeout and cache size. */
  public static WsConfig create(String url) {
    return create(url, DEFAULT_TIMEOUT, DEFAULT_CACHE_SIZE_MB);
  }

  /** Creates a {@link WsConfig} from a url and uses default timeout and cache size. */
  public static WsConfig create(String url, String timeout, String cacheSize) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "url is required");
    return new WsConfig(url, timeout, cacheSize);
  }

  /** Creates a {@link WsConfig} from a url and uses default timeout and cache size. */
  public static WsConfig create(String url, long timeout, long cacheSize) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "url is required");
    return new WsConfig(url, timeout, cacheSize);
  }

  /** */
  private static Properties loadProperties(Path propertiesPath) {
    Function<Path, InputStream> absolute =
        path -> {
          try {
            return new FileInputStream(path.toFile());
          } catch (Exception ex) {
            String msg = "Properties with absolute path could not be read from " + propertiesPath;
            throw new IORuntimeException(msg, ex);
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
      throw new IORuntimeException(msg, ex);
    }

    return props;
  }
}
