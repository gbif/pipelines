package au.org.ala.utils;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.ws.ClientConfiguration;
import java.io.IOException;
import java.net.URL;
import lombok.NonNull;
import org.gbif.pipelines.parsers.config.model.WsConfig;

/** Utilities for configuring web services that use the {@link au.org.ala.ws} package. */
public class WsUtils {
  /**
   * Construct an OkHTTP client configuration from a pipeline web-service configuration.
   *
   * <p>The pipelinesConfig is not used at present but is available for global defaults such as
   * cache directories, if these ever become a thing. </[>
   *
   * @param wsConfig The specific web service configuration
   * @param pipelinesConfig The full pipelines configuration, null to use defaults.
   * @return A corresponding client configuration
   * @throws IOException if unable to create the configuration
   */
  public static ClientConfiguration createConfiguration(
      @NonNull WsConfig wsConfig, ALAPipelinesConfig pipelinesConfig) throws IOException {
    return ClientConfiguration.builder()
        .baseUrl(new URL(wsConfig.getWsUrl()))
        .cache(wsConfig.getCacheSizeMb() > 0)
        .cacheSize(wsConfig.getCacheSizeMb() * 1024 * 1024)
        .timeOut(wsConfig.getTimeoutSec() * 1000)
        .build();
  }
}
