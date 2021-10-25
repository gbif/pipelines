package org.gbif.validator.ws.security;

import java.time.Duration;
import lombok.SneakyThrows;
import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.RemoteAuthWebSecurityConfigurer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration for all data sources, MyBatis mappers and services required by the Registry
 * security modules.
 */
@Configuration
public class RegistrySecurityConfiguration {

  @Bean
  public IdentityServiceClient identityServiceClient(
      @Value("${registry.ws.url}") String gbifApiUrl,
      @Value("${gbif.ws.security.appKey}") String appKey,
      @Value("${gbif.ws.security.appSecret}") String appSecret) {
    return IdentityServiceClient.getInstance(gbifApiUrl, appKey, appKey, appSecret);
  }

  @Bean
  public RestTemplate restTemplate(
      RestTemplateBuilder builder, @Value("${registry.ws.url}") String gbifApiUrl) {
    return builder
        .setConnectTimeout(Duration.ofSeconds(30))
        .setReadTimeout(Duration.ofSeconds(60))
        .rootUri(gbifApiUrl)
        .additionalInterceptors(
            (request, body, execution) -> {
              request.getHeaders().setContentType(MediaType.APPLICATION_JSON);
              return execution.execute(request, body);
            })
        .build();
  }

  @Configuration
  @EnableWebSecurity
  public static class ValidatorWebSecurity extends RemoteAuthWebSecurityConfigurer {

    @SneakyThrows
    public ValidatorWebSecurity(ApplicationContext applicationContext, RestTemplate restTemplate) {
      super(applicationContext, restTemplate);
    }
  }
}
