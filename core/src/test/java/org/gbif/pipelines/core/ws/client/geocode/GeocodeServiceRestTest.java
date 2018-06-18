package org.gbif.pipelines.core.ws.client.geocode;

import org.gbif.api.vocabulary.Country;
import org.gbif.pipelines.core.ws.MockServer;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;

/**
 * Tests the {@link GeocodeServiceRestTest} and {@link GeocodeService}.
 */
public class GeocodeServiceRestTest extends MockServer {

  @Test
  public void simpleCallTest() throws IOException {
    // @formatter:off
    // Call mocked: https://api.gbif-uat.org/v1/geocode/reverse?lat=60.4&lng=-131.3
    // @formatter:on

    enqueueResponse(CANADA_REVERSE_RESPONSE);

    GeocodeService service = GeocodeServiceRest.getInstance().getService();

    Call<Collection<GeocodeResponse>> call = service.reverse(LATITUDE_CANADA, LONGITUDE_CANADA);

    try {
      Response<Collection<GeocodeResponse>> response = call.execute();

      Assert.assertNotNull(response);

      Collection<GeocodeResponse> geocodeResponseCol = response.body();

      GeocodeResponse geocodeResponse = Iterables.get(geocodeResponseCol, 0);

      Assert.assertEquals(Country.CANADA.getTitle(), geocodeResponse.getCountryName());
      Assert.assertEquals(Country.CANADA.getIso2LetterCode(), geocodeResponse.getIsoCountryCode2Digit());

      ObjectMapper mapper = new ObjectMapper();
      System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(geocodeResponse));

    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }

  }

  @Test
  public void nullParamsCallTest() {
    GeocodeService service = GeocodeServiceRest.getInstance().getService();

    enqueueErrorResponse(HttpURLConnection.HTTP_BAD_REQUEST);

    Call<Collection<GeocodeResponse>> call = service.reverse(null, null);

    try {
      Response<Collection<GeocodeResponse>> response = call.execute();

      Assert.assertNull(response.body());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  @Test
  public void invalidParamsCallTest() {
    GeocodeService service = GeocodeServiceRest.getInstance().getService();

    enqueueErrorResponse(HttpURLConnection.HTTP_BAD_REQUEST);

    Call<Collection<GeocodeResponse>> call = service.reverse(300d, 300d);

    try {
      Response<Collection<GeocodeResponse>> response = call.execute();

      Assert.assertNull(response.body());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

}
