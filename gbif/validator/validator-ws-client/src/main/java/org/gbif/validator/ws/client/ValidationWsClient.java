package org.gbif.validator.ws.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import feign.*;
import feign.auth.BasicAuthRequestInterceptor;
import feign.httpclient.ApacheHttpClient;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Dataset;
import org.gbif.validator.api.Validation;
import org.gbif.validator.api.ValidationRequest;
import org.gbif.validator.api.ValidationSearchRequest;
import org.gbif.validator.service.ValidationService;

public interface ValidationWsClient extends ValidationService<File> {

  @RequestLine("POST /validation")
  @Headers("Content-Type: multipart/form-data")
  Validation validateFile(@Param("file") File file, @QueryMap ValidationRequest validationRequest);

  @RequestLine("POST /validation/url")
  @Headers("Content-Type: multipart/form-data")
  Validation validateFileFromUrl(
      @Param("fileUrl") String fileUrl, @QueryMap ValidationRequest validationRequest);

  @RequestLine("GET /validation")
  PagingResponse<Validation> list(@QueryMap Map<String, Object> validationSearchRequest);

  @RequestLine("GET /validation/{key}")
  Validation get(@Param("key") UUID key);

  @RequestLine("PUT /validation/{key}")
  @Headers("Content-Type: application/json")
  Validation update(@Param("key") UUID key, Validation validation);

  @RequestLine("PUT /validation/{key}/cancel")
  Validation cancel(@Param("key") UUID key);

  @RequestLine("DELETE /validation/{key}")
  void delete(@Param("key") UUID key);

  @RequestLine("GET /validation/{key}/eml")
  Dataset getDataset(@Param("key") UUID key);

  @RequestLine("GET /validation/running?min={min}")
  List<UUID> getRunningValidations(@Param("min") int min);

  @Override
  default PagingResponse<Validation> list(ValidationSearchRequest validationSearchRequest) {
    return list(ClientValidationSearchRequest.toQueryMap(validationSearchRequest));
  }

  @Override
  default Validation update(Validation validation) {
    return update(validation.getKey(), validation);
  }

  /** Uploads a file and starts the validation process. */
  default Validation submitFile(File file) {
    return validateFile(file, ValidationRequest.builder().build());
  }

  /** Default factory method for the ValidationWsClient. */
  static ValidationWsClient getInstance(String url, String userName, String password) {
    CloseableHttpClient httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();
    ObjectMapper mapper = new ObjectMapper();
    // initialise Validation client to send status updates
    return Feign.builder()
        // Reuse the timeout-configured http client (60s connect/socket) so a slow or
        // unreachable validation service fails instead of blocking the consumer thread.
        .client(new ApacheHttpClient(httpClient))
        .decoder(new JacksonDecoder(mapper))
        .encoder(new JacksonEncoder(mapper))
        .contract(new Contract.Default())
        .requestInterceptor(new BasicAuthRequestInterceptor(userName, password))
        .dismiss404()
        .target(ValidationWsClient.class, url);
  }
}
