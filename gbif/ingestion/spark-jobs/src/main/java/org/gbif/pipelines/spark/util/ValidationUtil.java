package org.gbif.pipelines.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import feign.Contract;
import feign.Feign;
import feign.auth.BasicAuthRequestInterceptor;
import feign.httpclient.ApacheHttpClient;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import java.util.Optional;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gbif.pipelines.core.config.model.PipelinesConfig;
import org.gbif.validator.api.Metrics;
import org.gbif.validator.api.Validation;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ValidationUtil {

  static ObjectMapper objectMapper = new ObjectMapper();

  public static ValidationClient createValidationClient(PipelinesConfig config) {
    CloseableHttpClient httpClient =
        HttpClients.custom()
            .setDefaultRequestConfig(
                RequestConfig.custom().setConnectTimeout(60_000).setSocketTimeout(60_000).build())
            .build();
    // initialise Validation client to send status updates
    return Feign.builder()
        // Reuse the timeout-configured http client (60s connect/socket) so a slow or
        // unreachable validation service fails instead of blocking the consumer thread.
        .client(new ApacheHttpClient(httpClient))
        .decoder(new JacksonDecoder(objectMapper))
        .encoder(new JacksonEncoder(objectMapper))
        .contract(new Contract.Default())
        .requestInterceptor(
            new BasicAuthRequestInterceptor(
                config.getStandalone().getRegistry().getUser(),
                config.getStandalone().getRegistry().getPassword()))
        .dismiss404()
        .target(ValidationClient.class, config.getStandalone().getRegistry().getWsUrl());
  }

  public static void updateMetrics(
      RetryingValidationClient validationClient, UUID key, Metrics generatedMetrics) {

    Validation validation = validationClient.get(key);
    if (validation == null) {
      log.warn("Can't find validation data key {}, please check that record exists", key);
      return;
    }
    Metrics metrics =
        Optional.ofNullable(validation.getMetrics()).orElse(Metrics.builder().build());
    metrics.setFileInfos(generatedMetrics.getFileInfos());
    // if we have made it to metrics, it should be indexable
    metrics.setIndexeable(true);
    validationClient.update(key, validation);
  }
}
