package org.gbif.pipelines.parsers.ws.client.geocode;

import java.util.Collection;
import java.util.Map;

import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;
import retrofit2.http.QueryMap;

/** API of the geocode WS. */
public interface GeocodeService {

  @GET("/v1/geocode/reverse")
  Call<Collection<GeocodeResponse>> reverse(@Query("lat") Double latitude, @Query("lng") Double longitude);

  @GET("/v1/geocode/reverse")
  Call<Collection<GeocodeResponse>> reverse(@QueryMap Map<String, String> params);
}
