package org.gbif.validator.orchestrator.config;

import org.gbif.common.messaging.MessageListener;
import org.gbif.validator.orchestrator.service.InitialValidationCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Consumes the initial request sent by validator-ws. */
@Configuration
public class InitialValidationListenerConfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(InitialValidationListenerConfiguration.class);

  @Bean
  public ApplicationRunner initialValidationListener(
      MessageListener listener,
      ListenMessagingProperties properties,
      InitialValidationCallback callback) {

    return args -> {
      listener.listen(
          properties.getQueue(),
          properties.getRoutingKey(),
          properties.getExchange(),
          properties.getThreads(),
          callback);
      LOG.info(
          "Listening for validation requests queue={} exchange={} routingKey={} threads={}",
          properties.getQueue(),
          properties.getExchange(),
          properties.getRoutingKey(),
          properties.getThreads());
    };
  }
}
