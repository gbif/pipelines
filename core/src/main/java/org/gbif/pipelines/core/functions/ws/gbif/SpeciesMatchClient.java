package org.gbif.pipelines.core.functions.ws.gbif;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.google.gson.annotations.SerializedName;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.Query;

/**
 * A demonstration of the GBIF species/match client
 */
public class SpeciesMatchClient {



  static class SpeciesMatch {
    private int usageKey;
    private String scientificName;
    private String canonicalName;
    private String rank;
    private String status;
    private int confidence;
    private String note;
    private String matchType;
    private SpeciesMatch[] alternatives;
    private String kingdom;
    private String phylum;
    private String order;
    private String family;
    private String genus;
    private int kingdomKey;
    private int phylumKey;
    private int classKey;
    private int orderKey;
    private int familyKey;
    private int genusKey;
    private boolean synonym;
    @SerializedName("class")
    private String class_;
  }

  interface SpeciesMatchService {
    @GET("/v1/species/match")
    Call<SpeciesMatch> match(
      @Query("kingdom") String kingdom,
      @Query("phylum") String phylum,
      @Query("class") String class_,
      @Query("order") String order,
      @Query("family") String family,
      @Query("genus") String genus,
      @Query("rank") String rank,
      @Query("name") String name,
      @Query("verbose") boolean verbose,
      @Query("strict") boolean strict);
  }

  public static void main(String[] args) throws IOException {
    //HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor();
    //interceptor.setLevel(HttpLoggingInterceptor.Level.BODY);

    // use a new file cache for the current session
    File httpCacheDirectory = Files.createTempDirectory("speciesMatchWS-cache").toFile();
    Cache cache = new Cache(httpCacheDirectory, 100 * 1024 * 1024); // 100MB file cache

    OkHttpClient client = new OkHttpClient.Builder()
      //.addInterceptor(interceptor)
      .cache(cache)
      .build();

    Retrofit retrofit = new Retrofit.Builder()
      .client(client)
      .baseUrl("https://api.gbif.org/")
      .addConverterFactory(GsonConverterFactory.create())
      .build();

    SpeciesMatchService service = retrofit.create(SpeciesMatchService.class);

    // call a ton of times, verifying on Varnish only 1 request will be seen(!)
    for (int i=0; i<100000; i++) {
      Call<SpeciesMatch> call = service.match(null, null, null, null, null, null, null, "Puma concolor", true, false);
      Response<SpeciesMatch> response = call.execute();
      System.out.println(response.body().scientificName);
    }
  }
}
