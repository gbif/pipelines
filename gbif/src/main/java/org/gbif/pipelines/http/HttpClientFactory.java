package org.gbif.pipelines.http;

import org.gbif.pipelines.http.config.Config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import okhttp3.Cache;
import okhttp3.OkHttpClient;

/**
 * Utility class for the creation of WS.
 */
public final class HttpClientFactory {

  private HttpClientFactory() {}

  /**
   * Creates a {@link OkHttpClient} with {@link Cache} from a specific {@link Config}.
   */
  public static OkHttpClient createClientWithCache(Config wsConfig) {
    // create cache file
    File httpCacheDirectory;
    try {
      // use a new file cache for the current session
      httpCacheDirectory = Files.createTempDirectory(wsConfig.getCacheConfig().getName()).toFile();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot run without the ability to create temporary cache directory", e);
    }

    // create cache
    Cache cache = new Cache(httpCacheDirectory, wsConfig.getCacheConfig().getSize());

    // create the client and return it
    return new OkHttpClient.Builder().connectTimeout(wsConfig.getTimeout(), TimeUnit.SECONDS)
      .readTimeout(wsConfig.getTimeout(), TimeUnit.SECONDS)
      .cache(cache)
      .build();
  }

}
