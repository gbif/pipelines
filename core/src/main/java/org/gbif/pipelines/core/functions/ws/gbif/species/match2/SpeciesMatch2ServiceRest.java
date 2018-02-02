package org.gbif.pipelines.core.functions.ws.gbif.species.match2;

import org.gbif.pipelines.core.functions.ws.config.Config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import okhttp3.Cache;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/*
 * Singleton to create the species match 2 service.
 */
public enum SpeciesMatch2ServiceRest {

  SINGLE;

  // TODO: receive as parameter
  private static final String PROPERTIES_FILE_PATH_DEFAULT = "species-ws.properties";

  private SpeciesMatch2Service service;

  SpeciesMatch2ServiceRest() {
    init();
  }

  public SpeciesMatch2Service getService() {
    return service;
  }

  void init() {
    // load WS Config
    Config wsConfig = new Config(PROPERTIES_FILE_PATH_DEFAULT);

    // create cache file
    File httpCacheDirectory;
    try {
      // use a new file cache for the current session
      httpCacheDirectory = Files.createTempDirectory(wsConfig.getCacheConfig().getName()).toFile();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot run without the ability to create temporary cache directory", e);
    }

    // create client
    Cache cache = new Cache(httpCacheDirectory, wsConfig.getCacheConfig().getSize());
    OkHttpClient client = new OkHttpClient.Builder().connectTimeout(wsConfig.getTimeout(), TimeUnit.SECONDS)
      .readTimeout(wsConfig.getTimeout(), TimeUnit.SECONDS)
      .cache(cache)
      .build();

    // create service
    Retrofit retrofit = new Retrofit.Builder().client(client)
      .baseUrl(wsConfig.getBasePath())
      .addConverterFactory(GsonConverterFactory.create())
      .build();

    service = retrofit.create(SpeciesMatch2Service.class);

  }

}
