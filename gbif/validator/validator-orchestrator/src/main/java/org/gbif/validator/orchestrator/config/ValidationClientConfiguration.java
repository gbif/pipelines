package org.gbif.validator.orchestrator.config;

import org.gbif.validator.ws.client.ValidationWsClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ValidationClientConfiguration {
  @Bean
  public ValidationWsClient validationWsClient(
      @Value("${orchestrator.validator-ws.url}") String url,
      @Value("${orchestrator.validator-ws.username}") String username,
      @Value("${orchestrator.validator-ws.password}") String password) {
    return ValidationWsClient.getInstance(url, username, password);
  }
}
