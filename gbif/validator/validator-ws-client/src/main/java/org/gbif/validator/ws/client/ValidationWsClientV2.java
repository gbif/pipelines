package org.gbif.validator.ws.client;

import org.gbif.validator.api.ValidationApi;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

public final class ValidationWsClientV2 {

  private ValidationWsClientV2() {}

  public static ValidationApi getInstance(String url, String username, String password) {

    RestClient restClient =
        RestClient.builder()
            .baseUrl(url)
            .defaultHeaders(headers -> headers.setBasicAuth(username, password))
            .build();

    RestClientAdapter adapter = RestClientAdapter.create(restClient);

    HttpServiceProxyFactory factory = HttpServiceProxyFactory.builderFor(adapter).build();

    return factory.createClient(ValidationApi.class);
  }
}
