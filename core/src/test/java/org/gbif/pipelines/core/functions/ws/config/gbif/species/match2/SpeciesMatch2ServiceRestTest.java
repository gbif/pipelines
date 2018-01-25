package org.gbif.pipelines.core.functions.ws.config.gbif.species.match2;

import org.gbif.pipelines.core.functions.ws.gbif.species.match2.SpeciesMatch2Service;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.SpeciesMatch2ServiceRest;
import org.gbif.pipelines.core.functions.ws.gbif.species.match2.model.SpeciesMatch2ResponseModel;

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

    Call<SpeciesMatch2ResponseModel> call =
      service.match2(null, null, null, null, null, null, null, "Puma " + "concolor", true, false);

    try {
      Response<SpeciesMatch2ResponseModel> response = call.execute();

      Gson gson = new GsonBuilder().setPrettyPrinting().create();
      System.out.println(gson.toJson(response.body()));

    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

  }

}
