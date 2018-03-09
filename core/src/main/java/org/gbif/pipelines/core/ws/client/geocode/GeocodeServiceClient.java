package org.gbif.pipelines.core.ws.client.geocode;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.pipelines.core.ws.HttpResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;

import static org.gbif.pipelines.core.parsers.location.CoordinatesValidator.isAntarctica;
import static org.gbif.pipelines.core.parsers.location.CoordinatesValidator.isInRange;

public class GeocodeServiceClient {

  private static final Logger LOG = LoggerFactory.getLogger(GeocodeServiceClient.class);

  private GeocodeServiceClient() {}

  public static HttpResponse<List<Country>> getCountriesFromLatLng(LatLng latLng, Country country) {
    if (!isInRange(latLng)) {
      return HttpResponse.<List<Country>>fail(null, "lat and lng out of range", HttpResponse.ErrorCode.ABORTED);
    }

    GeocodeService service = GeocodeServiceRest.getInstance().getService();

    Call<Collection<GeocodeResponse>> call = service.reverse(latLng.getLat(), latLng.getLng());

    // TODO: extract to base class and use it for all the ws?? only in case that code is common
    try {
      Response<Collection<GeocodeResponse>> response = call.execute();

      if (!response.isSuccessful()) {
        String errorMessage = "Call to geocode reverse WS failed: " + response.message();
        return HttpResponse.<List<Country>>fail(Collections.emptyList(),
                                                response.code(),
                                                errorMessage,
                                                HttpResponse.ErrorCode.CALL_FAILED);
      }

      return HttpResponse.<List<Country>>success(parseResponse(response.body(), latLng, country));
    } catch (IOException e) {
      LOG.error("Error calling the geocode reverse WS", e);
      String errorMessage = "Error calling the geocode reverse WS";
      // TODO: new constructor not to send null for the body??
      return HttpResponse.<List<Country>>fail(null, errorMessage, HttpResponse.ErrorCode.UNEXPECTED_ERROR);
    }
  }

  private static List<Country> parseResponse(Collection<GeocodeResponse> response, LatLng latLng, Country country) {
    if (response != null && !response.isEmpty()) {
      return response.stream()
        .filter(resp -> resp.getIsoCountryCode2Digit() != null)
        .map(resp -> Country.fromIsoCode(resp.getIsoCountryCode2Digit()))
        .collect(Collectors.toList());
    }

    if (isAntarctica(latLng.getLat(), country)) {
      return Collections.singletonList(Country.ANTARCTICA);
    }

    return Collections.emptyList();
  }

}
