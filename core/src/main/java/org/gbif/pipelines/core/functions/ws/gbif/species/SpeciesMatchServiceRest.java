package org.gbif.pipelines.core.functions.ws.gbif.species;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

/*
 * Singleton realisation
 * */
enum SpeciesMatchServiceRest {

  SINGLE;

  //TODO: Move to properties
  private static final String CACHE_NAME = "speciesMatchWS-cache";
  private static final long CACHE_SIZE = 100 * 1024 * 1024; //100Mb
  private static final long TIMEOUT = 60;
  private static final String WS_PATH = "https://api.gbif.org/";

  private SpeciesMatchService service;

  SpeciesMatchServiceRest() {
    init();
  }

  SpeciesMatchService getService() {
    return service;
  }

  void init() {
    File httpCacheDirectory;
    try {
      // use a new file cache for the current session
      httpCacheDirectory = Files.createTempDirectory(CACHE_NAME).toFile();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot run without the ability to create temporary cache directory", e);
    }

    Cache cache = new Cache(httpCacheDirectory, CACHE_SIZE);
    OkHttpClient client = new OkHttpClient.Builder()
        .connectTimeout(TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(TIMEOUT, TimeUnit.SECONDS)
        .cache(cache)
        .build();

    Retrofit retrofit = new Retrofit.Builder()
        .client(client)
        .baseUrl(WS_PATH)
        .addConverterFactory(GsonConverterFactory.create())
        .build();

    service = retrofit.create(SpeciesMatchService.class);

  }

}
