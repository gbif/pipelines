package au.org.ala.kvs.client.retrofit;

import au.org.ala.kvs.client.ALANameUsageMatch;
import au.org.ala.kvs.client.ALASpeciesMatchRequest;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.Headers;
import retrofit2.http.POST;

/** ALA name matching Retrofit Service client. */
interface ALANameUsageMatchRetrofitService {

  @POST("/api/searchByClassification")
  @Headers({"Content-Type: application/json"})
  Call<ALANameUsageMatch> match(@Body ALASpeciesMatchRequest nameMatch);
}
