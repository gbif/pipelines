package org.gbif.pipelines.core.ws.client.geocode;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.core.ws.client.BaseServiceClient;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import retrofit2.Call;

import static org.gbif.pipelines.core.parsers.location.CoordinatesValidator.isInRange;

public class GeocodeServiceClient extends BaseServiceClient<Collection<GeocodeResponse>, List<Country>> {

  private GeocodeServiceClient() {}

  public static GeocodeServiceClient getInstance() {
    return new GeocodeServiceClient();
  }

  public HttpResponse<List<Country>> getCountriesFromLatLng(LatLng latLng) {
    if (!isInRange(latLng)) {
      return HttpResponse.fail("lat and lng out of range", HttpResponse.ErrorCode.ABORTED);
    }

    return performCall(createParamsMap(latLng));
  }

  private static Map<String, String> createParamsMap(LatLng latLng) {
    Map<String, String> params = new HashMap<>();
    params.put("lat", String.valueOf(latLng.getLat()));
    params.put("lng", String.valueOf(latLng.getLat()));

    return params;
  }

  @Override
  protected Call<Collection<GeocodeResponse>> getCall(Map<String, String> params) {
    return GeocodeServiceRest.getInstance().getService().reverse(params);
  }

  @Override
  protected String getErrorMessage() {
    return "Error calling the geocode reverse WS";
  }

  @Override
  protected List<Country> parseResponse(Collection<GeocodeResponse> response) {
    if (response != null && !response.isEmpty()) {
      return response.stream()
        .filter(resp -> resp.getIsoCountryCode2Digit() != null)
        .map(resp -> Country.fromIsoCode(resp.getIsoCountryCode2Digit()))
        .collect(Collectors.toList());
    }

    return Collections.emptyList();
  }

}
