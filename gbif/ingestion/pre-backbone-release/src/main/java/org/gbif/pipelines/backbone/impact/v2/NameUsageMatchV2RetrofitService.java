package org.gbif.pipelines.backbone.impact.v2;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface NameUsageMatchV2RetrofitService {
  @GET("v2/species/match")
  Call<NameUsageMatchV2> match(
      @Query("taxonID") String taxonID,
      @Query("taxonConceptID") String taxonConceptID,
      @Query("scientificNameID") String scientificNameID,
      @Query("scientificName") String scientificName,
      @Query("scientificNameAuthorship") String authorship,
      @Query("taxonRank") String rank,
      @Query("genericName") String genericName,
      @Query("specificEpithet") String specificEpithet,
      @Query("infraspecificEpithet") String infraspecificEpithet,
      @Query("kingdom") String kingdom,
      @Query("phylum") String phylum,
      @Query("class") String clazz,
      @Query("order") String order,
      @Query("family") String family,
      @Query("genus") String genus,
      @Query("strict") Boolean strict,
      @Query("verbose") Boolean verbose);
}
