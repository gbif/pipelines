package org.gbif.validator.orchestrator.config;

import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

/** Allows internal monitoring without credentials while denying other HTTP requests. */
@Configuration(proxyBeanMethods = false)
public class ActuatorSecurityConfiguration {

  @Bean
  public SecurityFilterChain actuatorSecurity(HttpSecurity http) throws Exception {
    return http.authorizeHttpRequests(
            authorization ->
                authorization
                    .requestMatchers(EndpointRequest.toAnyEndpoint())
                    .permitAll()
                    .anyRequest()
                    .denyAll())
        .build();
  }
}
