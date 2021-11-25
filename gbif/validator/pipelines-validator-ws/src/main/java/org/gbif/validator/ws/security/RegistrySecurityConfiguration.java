package org.gbif.validator.ws.security;

import lombok.SneakyThrows;
import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.RemoteAuthClient;
import org.gbif.ws.remoteauth.RemoteAuthWebSecurityConfigurer;
import org.gbif.ws.remoteauth.RestTemplateRemoteAuthClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

/**
 * Configuration for all data sources, MyBatis mappers and services required by the Registry
 * security modules.
 */
@Configuration
public class RegistrySecurityConfiguration {

  @Bean
  public IdentityServiceClient identityServiceClient(
      @Value("${gbif.api.url}") String gbifApiUrl,
      @Value("${gbif.ws.security.appKey}") String appKey,
      @Value("${gbif.ws.security.appSecret}") String appSecret) {
    return IdentityServiceClient.getInstance(gbifApiUrl, appKey, appKey, appSecret);
  }

  @Bean
  public RemoteAuthClient remoteAuthClient(
      RestTemplateBuilder builder, @Value("${gbif.api.url}") String gbifApiUrl) {
    return RestTemplateRemoteAuthClient.createInstance(builder, gbifApiUrl);
  }

  @Configuration
  @EnableWebSecurity
  public static class ValidatorWebSecurity extends RemoteAuthWebSecurityConfigurer {

    @SneakyThrows
    public ValidatorWebSecurity(
        ApplicationContext applicationContext, RemoteAuthClient remoteAuthClient) {
      super(applicationContext, remoteAuthClient);
    }
  }
}
