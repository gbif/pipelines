package org.gbif.validator.ws.security;

import lombok.SneakyThrows;
import org.gbif.ws.security.remote.IdentityServiceClient;
import org.gbif.ws.security.remote.RemoteAuthWebSecurityConfigurer;
import org.springframework.beans.factory.annotation.Value;
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
      @Value("${registry.ws.url}") String gbifApiUrl,
      @Value("${gbif.ws.security.appKey}") String appKey,
      @Value("${gbif.ws.security.appSecret}") String appSecret) {
    return IdentityServiceClient.getInstance(gbifApiUrl, appKey, appKey, appSecret);
  }

  @Configuration
  @EnableWebSecurity
  public static class ValidatorWebSecurity extends RemoteAuthWebSecurityConfigurer {

    @SneakyThrows
    public ValidatorWebSecurity(
        ApplicationContext applicationContext, IdentityServiceClient identityServiceClient) {
      super(applicationContext, identityServiceClient);
    }
  }
}
