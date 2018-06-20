package org.gbif.pipelines.core.ws.client.geocode;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.pipelines.core.ws.HttpResponse;
import org.gbif.pipelines.core.ws.client.BaseServiceClient;
import org.gbif.pipelines.core.ws.config.Config;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import retrofit2.Call;

import static org.gbif.pipelines.core.parsers.location.CoordinatesValidator.isInRange;

public class GeocodeServiceClient
    extends BaseServiceClient<Collection<GeocodeResponse>, List<Country>> {

  private final GeocodeServiceRest geocodeServiceRest;

  private GeocodeServiceClient(Config wsConfig) {
    geocodeServiceRest = GeocodeServiceRest.getInstance(wsConfig);
  }

  /**
   * It creates an instance of {@link GeocodeServiceClient} reading the ws configuration from the
   * path received.
   */
  public static GeocodeServiceClient newInstance(Config wsConfig) {
    Objects.requireNonNull(wsConfig, "WS config is required");
    return new GeocodeServiceClient(wsConfig);
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
    params.put("lng", String.valueOf(latLng.getLng()));

    return params;
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
    if (Objects.nonNull(response) && !response.isEmpty()) {
      return response
          .stream()
          .filter(resp -> Objects.nonNull(resp.getIsoCountryCode2Digit()))
          .map(resp -> Country.fromIsoCode(resp.getIsoCountryCode2Digit()))
          .collect(Collectors.toList());
    }

    return Collections.emptyList();
  }
}
