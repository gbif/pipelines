package org.gbif.pipelines.core.functions.ws.gbif.species;

import org.gbif.pipelines.io.avro.TypedOccurrence;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

// WS API
interface SpeciesMatchService {

  @GET("/v1/species/match")
  Call<SpeciesMatchResponseModel> match(
      @Query("kingdom") String kingdom,
      @Query("phylum") String phylum,
      @Query("class") String clazz,
      @Query("order") String order,
      @Query("family") String family,
      @Query("genus") String genus,
      @Query("rank") String rank,
      @Query("name") String name,
      @Query("verbose") boolean verbose,
      @Query("strict") boolean strict);

  default Call<SpeciesMatchResponseModel> match(TypedOccurrence to) {
    return match(
        to.getKingdom(),
        to.getPhylum(),
        to.getClass$(),
        to.getOrder(),
        to.getFamily(),
        to.getGenus(),
        to.getSpecies(),
        to.getScientificName(),
        false, // i.e. not verbose
        false); // i.e. not strict
  }
}