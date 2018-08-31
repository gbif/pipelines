package org.gbif.pipelines.parsers.ws.client.geocode;

import org.gbif.api.vocabulary.Country;
import org.gbif.pipelines.parsers.ws.BaseMockServerTest;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collection;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import retrofit2.Call;
import retrofit2.Response;

/** Tests the {@link GeocodeServiceRestTest} and {@link GeocodeService}. */
public class GeocodeServiceRestTest extends BaseMockServerTest {

  /** Call mocked: https://api.gbif-uat.org/v1/geocode/reverse?lat=60.4&lng=-131.3 */
  @Test
  public void simpleCallTest() throws IOException {

    // State
    enqueueResponse(CANADA_REVERSE_RESPONSE);
    GeocodeService service = GeocodeServiceRest.getInstance(getWsConfig()).getService();

    // When
    Call<Collection<GeocodeResponse>> call = service.reverse(LATITUDE_CANADA, LONGITUDE_CANADA);
    Response<Collection<GeocodeResponse>> response = call.execute();

    Collection<GeocodeResponse> geocodeResponseCol = response.body();
    GeocodeResponse geocodeResponse = Iterables.get(geocodeResponseCol, 0);

    // Should
    Assert.assertNotNull(response);
    Assert.assertEquals(Country.CANADA.getTitle(), geocodeResponse.getCountryName());
    Assert.assertEquals(
        Country.CANADA.getIso2LetterCode(), geocodeResponse.getIsoCountryCode2Digit());
  }

  @Test
  public void nullParamsCallTest() throws IOException {

    // State
    GeocodeService service = GeocodeServiceRest.getInstance(getWsConfig()).getService();
    enqueueErrorResponse(HttpURLConnection.HTTP_BAD_REQUEST);

    // When
    Call<Collection<GeocodeResponse>> call = service.reverse(null, null);
    Response<Collection<GeocodeResponse>> response = call.execute();

    // Should
    Assert.assertNull(response.body());
  }

  @Test
  public void invalidParamsCallTest() throws IOException {

    // State
    GeocodeService service = GeocodeServiceRest.getInstance(getWsConfig()).getService();
    enqueueErrorResponse(HttpURLConnection.HTTP_BAD_REQUEST);

    // When
    Call<Collection<GeocodeResponse>> call = service.reverse(300d, 300d);
    Response<Collection<GeocodeResponse>> response = call.execute();

    // Should
    Assert.assertNull(response.body());
  }
}
