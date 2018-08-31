package org.gbif.pipelines.parsers.ws.client.match2;

import org.gbif.api.v2.NameUsageMatch2;

import java.util.Map;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;
import retrofit2.http.QueryMap;

/** API of the species match 2 WS. */
public interface SpeciesMatchv2Service {

  @GET("/v1/species/match2")
  Call<NameUsageMatch2> match(@Query("kingdom") String kingdom, @Query("phylum") String phylum,
    @Query("class") String clazz, @Query("order") String order, @Query("family") String family,
    @Query("genus") String genus, @Query("rank") String rank, @Query("name") String name,
    @Query("verbose") boolean verbose, @Query("strict") boolean strict);

  @GET("/v1/species/match2")
  Call<NameUsageMatch2> match(@QueryMap Map<String, String> params);
}
