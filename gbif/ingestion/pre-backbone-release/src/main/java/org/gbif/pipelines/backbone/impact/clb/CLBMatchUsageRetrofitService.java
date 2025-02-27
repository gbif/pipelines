package org.gbif.pipelines.backbone.impact.clb;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Path;
import retrofit2.http.Query;

public interface CLBMatchUsageRetrofitService {
  @GET("dataset/{datasetKey}/match/nameusage")
  Call<CLBUsageMatch> match(
      @Path("datasetKey") String datasetKey,
      @Query("kingdom") String kingdom,
      @Query("phylum") String phylum,
      @Query("class") String clazz,
      @Query("order") String order,
      @Query("family") String family,
      @Query("genus") String genus,
      @Query("scientificName") String scientificName,
      @Query("authorship") String authorship,
      @Query("rank") String rank,
      @Query("verbose") boolean verbose);
}
