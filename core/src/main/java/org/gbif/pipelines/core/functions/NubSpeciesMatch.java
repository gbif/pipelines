package org.gbif.pipelines.core.functions;

import org.gbif.dwca.record.StarRecord;
import org.gbif.pipelines.core.functions.ws.gbif.SpeciesMatchClient;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TypedOccurrence;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import com.google.gson.annotations.SerializedName;
import okhttp3.Cache;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Query;

/**
 * DEMO IMPLEMENTATION ONLY! Much can be done to improve this...
 * A function to set the nub fields of the interpreted record.
 */
class NubSpeciesMatch implements SerializableFunction<TypedOccurrence, TypedOccurrence> {
  private static final String lock = new String();

  private String baseUrl;

  // instanciated on each task
  transient SpeciesMatchService service;

  private SpeciesMatchService getService() {
    if (service == null) {
      synchronized (lock) {
        try {
          // use a new file cache for the current session
          File httpCacheDirectory = Files.createTempDirectory("speciesMatchWS-cache").toFile();
          Cache cache = new Cache(httpCacheDirectory, 100 * 1024 * 1024); // 100MB file cache

          OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(60, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .cache(cache)
            .build();

          Retrofit retrofit = new Retrofit.Builder()
            .client(client)
            .baseUrl(baseUrl)
            .addConverterFactory(GsonConverterFactory.create())
            .build();

          service = retrofit.create(SpeciesMatchService.class);
        } catch (IOException e) {
          throw new IllegalStateException("Cannot run without the ability to create temporary cache directory", e);
        }
      }
    }
    return service;
  }

  NubSpeciesMatch(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  @Override
  public TypedOccurrence apply(TypedOccurrence s) {
    // here we are shockingly updating the data inline
    // TODO this should be reading from one object and returning another

      Call<SpeciesMatch> call = getService().match(
        valueOf(s.getKingdom()),
        valueOf(s.getPhylum()),
        valueOf(s.getClass$()),
        valueOf(s.getOrder()),
        valueOf(s.getFamily()),
        valueOf(s.getGenus()),
        valueOf(s.getSpecies()),
        valueOf(s.getScientificName()),
        false, // i.e. not verbose
        false); // i.e. not strict

    try {
      Response<SpeciesMatch> r = call.execute();

      if (r.isSuccessful()) {
        // lots to clean up here... compare with current code
        s.setKingdom(r.body().kingdom);
        s.setPhylum(r.body().phylum);
        s.setClass$(r.body().class_);
        s.setOrder(r.body().order);
        s.setFamily(r.body().family);
        s.setGenus(r.body().genus);
        s.setScientificName(r.body().scientificName);
        s.setKingdomKey(r.body().kingdomKey);
        s.setPhylumKey(r.body().phylumKey);
        s.setClassKey(r.body().classKey);
        s.setOrderKey(r.body().orderKey);
        s.setFamilyKey(r.body().familyKey);
        s.setGenusKey(r.body().genusKey);
        s.setNubKey(r.body().usageKey);
        s.setTaxonRank(r.body().rank);
      }

      return s;

    } catch (IOException e) {
      throw new RuntimeException("Unable to run species/match lookup", e);
    }
  }

  private static String valueOf(CharSequence c) {
    return c == null ? null : c.toString();
  }

  // Response for the WS call
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

  // WS API
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
}
