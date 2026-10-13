package org.gbif.validator.orchestrator.config;

import org.gbif.common.messaging.MessageListener;
import org.gbif.validator.orchestrator.service.InitialValidationCallback;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Consumes the initial request sent by validator-ws, when explicitly enabled. */
@Configuration
public class InitialValidationListenerConfiguration {

  @Bean
  @ConditionalOnProperty(name = "orchestrator.unsafe-enable-start-consumer", havingValue = "true")
  public ApplicationRunner initialValidationListener(
      MessageListener listener,
      StartMessagingProperties properties,
      InitialValidationCallback callback) {

    return args -> {
      listener.listen(
          properties.getQueue(),
          properties.getRoutingKey(),
          properties.getExchange(),
          properties.getThreads(),
          callback);
    };
  }
}
