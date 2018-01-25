package org.gbif.pipelines.core.functions.ws.gbif.species.match2;

import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.SpeciesMatch2ResponseModel;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

// WS API MATCH 2
public interface SpeciesMatch2Service {

  @GET("/v1/species/match2")
  Call<SpeciesMatch2ResponseModel> match2(
    @Query("kingdom") String kingdom,
    @Query("phylum") String phylum,
    @Query("class") String clazz,
    @Query("order") String order,
    @Query("family") String family,
    @Query("genus") String genus,
    @Query("rank") String rank,
    @Query("name") String name,
    @Query("verbose") boolean verbose,
    @Query("strict") boolean strict
  );
}