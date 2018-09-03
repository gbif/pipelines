package org.gbif.pipelines.parsers.ws;

import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import okhttp3.Cache;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory class for http client. */
public final class HttpClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HttpClientFactory.class);

  private HttpClientFactory() {}

  /** Creates a {@link OkHttpClient} with {@link Cache} from a specific {@link WsConfig}. */
  public static OkHttpClient createClient(WsConfig config) {
    // create cache file
    File httpCacheDirectory;
    try {
      // use a new file cache for the current session
      String cacheName = String.valueOf(System.currentTimeMillis()) + "-wsCache";
      httpCacheDirectory = Files.createTempDirectory(cacheName).toFile();
      httpCacheDirectory.deleteOnExit();
      LOG.info("Cache file created - {}", httpCacheDirectory.getAbsolutePath());
    } catch (IOException e) {
      throw new IllegalStateException(
          "Cannot run without the ability to create temporary cache directory", e);
    }

    // create cache
    Cache cache = new Cache(httpCacheDirectory, config.getCacheSize());

    // create the client and return it
    return new OkHttpClient.Builder()
        .connectTimeout(config.getTimeout(), TimeUnit.SECONDS)
        .readTimeout(config.getTimeout(), TimeUnit.SECONDS)
        .cache(cache)
        .build();
  }
}
