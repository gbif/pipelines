package org.gbif.pipelines.core.functions.ws.gbif.species;

import java.io.IOException;
import retrofit2.Call;
import retrofit2.Response;

/**
 * A demonstration of the GBIF species/match client
 */
public class SpeciesMatchClientDemo {

  public static void main(String[] args) throws IOException {
    //HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor();
    //interceptor.setLevel(HttpLoggingInterceptor.Level.BODY);

    // use a new file cache for the current session
    SpeciesMatchService service = SpeciesMatchServiceRest.SINGLE.getService();

    // call a ton of times, verifying on Varnish only 1 request will be seen(!)
    for (int i = 0; i < 100000; i++) {
      Call<SpeciesMatchResponseModel> call = service.match(null, null, null, null, null, null, null, "Puma concolor", true, false);
      Response<SpeciesMatchResponseModel> response = call.execute();
      System.out.println(response.body().getScientificName());
    }
  }
}
