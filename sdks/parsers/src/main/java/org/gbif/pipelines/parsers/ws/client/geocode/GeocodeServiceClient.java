package org.gbif.pipelines.parsers.ws.client.geocode;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.pipelines.parsers.ws.HttpResponse;
import org.gbif.pipelines.parsers.ws.client.BaseServiceClient;
import org.gbif.pipelines.parsers.ws.config.WsConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import retrofit2.Call;

public class GeocodeServiceClient
    extends BaseServiceClient<Collection<GeocodeResponse>, List<Country>> {

  private final GeocodeServiceRest geocodeServiceRest;

  private GeocodeServiceClient(WsConfig wsConfig) {
    geocodeServiceRest = GeocodeServiceRest.getInstance(wsConfig);
  }

  /**
   * It creates an instance of {@link GeocodeServiceClient} reading the ws configuration from the
   * path received.
   */
  public static GeocodeServiceClient create(WsConfig wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new GeocodeServiceClient(wsConfig);
  }

  public HttpResponse<List<Country>> getCountriesFromLatLng(LatLng latLng) {

    // Check range
    boolean isInRange =
        Double.compare(latLng.getLat(), 90) <= 0
            && Double.compare(latLng.getLat(), -90) >= 0
            && Double.compare(latLng.getLng(), 180) <= 0
            && Double.compare(latLng.getLng(), -180) >= 0;

    if (!isInRange) {
      return HttpResponse.fail("lat and lng out of range", HttpResponse.ErrorCode.ABORTED);
    }

    // Map lat and lng parameters
    Map<String, String> params = new HashMap<>();
    params.put("lat", String.valueOf(latLng.getLat()));
    params.put("lng", String.valueOf(latLng.getLng()));

    return performCall(params);
  }

  @Override
  protected Call<Collection<GeocodeResponse>> getCall(Map<String, String> params) {
    return geocodeServiceRest.getService().reverse(params);
  }

  @Override
  protected String getErrorMessage() {
    return "Error calling the geocode reverse WS";
  }

  @Override
  protected List<Country> parseResponse(Collection<GeocodeResponse> response) {
    if (response != null && !response.isEmpty()) {
      return response
          .stream()
          .filter(resp -> resp.getIsoCountryCode2Digit() != null)
          .map(resp -> Country.fromIsoCode(resp.getIsoCountryCode2Digit()))
          .collect(Collectors.toList());
    }

    return Collections.emptyList();
  }
}
