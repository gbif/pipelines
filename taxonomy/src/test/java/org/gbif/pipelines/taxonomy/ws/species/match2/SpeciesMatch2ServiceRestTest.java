package org.gbif.pipelines.taxonomy.ws.species.match2;

import org.gbif.api.v2.NameUsageMatch2;

import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;

public class SpeciesMatch2ServiceRestTest {

  @Test
  public void simpleCallTest() {
    SpeciesMatch2Service service = SpeciesMatch2ServiceRest.SINGLE.getService();

    final String name = "Puma concolor";

    Call<NameUsageMatch2> call = service.match2(null, null, null, null, null, null, null, name, true, false);

    try {
      Response<NameUsageMatch2> response = call.execute();

      Gson gson = new GsonBuilder().setPrettyPrinting().create();

      Assert.assertNotNull(response);

      System.out.println(gson.toJson(response.body()));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

  }

}
