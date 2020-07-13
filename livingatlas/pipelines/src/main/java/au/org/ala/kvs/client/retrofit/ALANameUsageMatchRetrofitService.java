package au.org.ala.kvs.client.retrofit;

import au.org.ala.kvs.client.ALANameUsageMatch;
import au.org.ala.kvs.client.ALASpeciesMatchRequest;
import org.gbif.rest.client.geocode.Location;
import org.gbif.rest.client.species.NameUsageMatch;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.Collection;

/**
 * ALA name matching Retrofit Service client.
 */
interface ALANameUsageMatchRetrofitService {

  @POST("/api/searchByClassification")
  @Headers({"Content-Type: application/json"})
  Call<ALANameUsageMatch> match(@Body ALASpeciesMatchRequest nameMatch);
}
